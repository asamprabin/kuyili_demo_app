import 'dart:async';
import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:record/record.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:permission_handler/permission_handler.dart';

class AudioStreamPage extends StatefulWidget {
  const AudioStreamPage({super.key});

  @override
  State<AudioStreamPage> createState() => _AudioStreamPageState();
}

class _AudioStreamPageState extends State<AudioStreamPage> {
  final AudioRecorder _recorder = AudioRecorder();
  WebSocketChannel? _channel;
  StreamSubscription<Uint8List>? _audioStreamSubscription;

  bool _isStreaming = false;
  int _bytesSent = 0;

  static const String _wsUrl = 'ws://192.168.1.130:8765';

  @override
  void dispose() {
    _stopStreaming();
    _recorder.dispose();
    super.dispose();
  }

  Future<bool> _requestMicPermission() async {
    final status = await Permission.microphone.request();
    return status.isGranted;
  }

  Future<void> _startStreaming() async {
    final hasPermission = await _requestMicPermission();
    if (!hasPermission) {
      _showSnack('Microphone permission denied');
      return;
    }

    // Check the record package's own permission check too
    if (!await _recorder.hasPermission()) {
      _showSnack('Microphone permission denied');
      return;
    }

    try {
      // Connect to the WebSocket server
      _channel = WebSocketChannel.connect(Uri.parse(_wsUrl));

      // Listen for messages coming back from the server (optional)
      _channel!.stream.listen(
        (message) {
          debugPrint('Received from server: $message');
        },
        onError: (error) {
          debugPrint('WebSocket error: $error');
          _stopStreaming();
        },
        onDone: () {
          debugPrint('WebSocket closed');
          _stopStreaming();
        },
      );

      // Configure audio: 16kHz mono PCM16 is a common format for
      // speech APIs (e.g. transcription services). Adjust as needed.
      const config = RecordConfig(
        encoder: AudioEncoder.pcm16bits,
        sampleRate: 16000,
        numChannels: 1,
      );

      // startStream returns a Stream<Uint8List> of raw audio chunks
      final stream = await _recorder.startStream(config);

      setState(() {
        _isStreaming = true;
        _bytesSent = 0;
      });

      _audioStreamSubscription = stream.listen(
        (chunk) {
          if (_channel != null) {
            _channel!.sink.add(chunk); // send binary audio data
            setState(() => _bytesSent += chunk.length);
          }
        },
        onError: (error) {
          debugPrint('Audio stream error: $error');
          _stopStreaming();
        },
      );
    } catch (e) {
      debugPrint('Failed to start streaming: $e');
      _showSnack('Failed to start streaming: $e');
      _stopStreaming();
    }
  }

  Future<void> _stopStreaming() async {
    await _audioStreamSubscription?.cancel();
    _audioStreamSubscription = null;

    if (await _recorder.isRecording()) {
      await _recorder.stop();
    }

    await _channel?.sink.close();
    _channel = null;

    if (mounted) {
      setState(() => _isStreaming = false);
    }
  }

  void _showSnack(String msg) {
    if (mounted) {
      ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(msg)));
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Mic Audio Streamer')),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(
              _isStreaming ? Icons.mic : Icons.mic_off,
              size: 80,
              color: _isStreaming ? Colors.red : Colors.grey,
            ),
            const SizedBox(height: 20),
            Text(_isStreaming ? 'Streaming...' : 'Not streaming'),
            const SizedBox(height: 10),
            Text('Bytes sent: $_bytesSent'),
            const SizedBox(height: 40),
            ElevatedButton(
              onPressed: _isStreaming ? _stopStreaming : _startStreaming,
              child: Text(_isStreaming ? 'Stop' : 'Start Streaming'),
            ),
          ],
        ),
      ),
    );
  }
}