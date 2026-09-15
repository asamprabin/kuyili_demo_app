import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:record/record.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:permission_handler/permission_handler.dart';
import 'package:audioplayers/audioplayers.dart'; // <-- Added import

class AudioStreamPage extends StatefulWidget {
  const AudioStreamPage({super.key});

  @override
  State<AudioStreamPage> createState() => _AudioStreamPageState();
}

class ChatMessage {
  final bool isUser;
  final String text;
  final bool isLoading;
  final Duration? responseTime;
  final DateTime timestamp;

  ChatMessage({
    required this.isUser,
    required this.text,
    this.isLoading = false,
    this.responseTime,
    DateTime? timestamp,
  }) : timestamp = timestamp ?? DateTime.now();

  ChatMessage copyWith({
    String? text,
    bool? isLoading,
    Duration? responseTime,
  }) {
    return ChatMessage(
      isUser: isUser,
      text: text ?? this.text,
      isLoading: isLoading ?? this.isLoading,
      responseTime: responseTime ?? this.responseTime,
      timestamp: timestamp,
    );
  }
}

class _AudioStreamPageState extends State<AudioStreamPage> {
  final AudioRecorder _recorder = AudioRecorder();
  final ScrollController _scrollController = ScrollController();
  final AudioPlayer _audioPlayer = AudioPlayer(); // <-- Added AudioPlayer

  WebSocketChannel? _channel;
  StreamSubscription<Uint8List>? _audioStreamSubscription;
  StreamSubscription? _wsSubscription;

  bool _isConnected = false;
  bool _isRecording = false;
  bool _waitingForReply = false;
  
  // Audio streaming state
  bool _isReceivingAudio = false;
  final List<int> _audioBuffer = [];

  Stopwatch? _responseStopwatch;
  Timer? _recordingTimer;
  Duration _recordingDuration = Duration.zero;

  final List<ChatMessage> _messages = [];

  static const String _wsUrl = 'ws://192.168.1.11:8765';

  @override
  void initState() {
    super.initState();
    _connect();
  }

  @override
  void dispose() {
    _recordingTimer?.cancel();
    _audioStreamSubscription?.cancel();
    _wsSubscription?.cancel();
    _channel?.sink.close();
    _recorder.dispose();
    _audioPlayer.dispose(); // <-- Clean up player
    _scrollController.dispose();
    super.dispose();
  }

  Future<bool> _requestMicPermission() async {
    final status = await Permission.microphone.request();
    return status.isGranted;
  }

  void _connect() {
    try {
      _channel = WebSocketChannel.connect(Uri.parse(_wsUrl));
      _wsSubscription = _channel!.stream.listen(
        _onServerMessage,
        onError: (error) {
          debugPrint('WebSocket error: $error');
          _showSnack('Connection error: $error');
          setState(() => _isConnected = false);
        },
        onDone: () {
          debugPrint('WebSocket closed');
          setState(() => _isConnected = false);
        },
      );
      setState(() => _isConnected = true);
    } catch (e) {
      debugPrint('Failed to connect: $e');
      _showSnack('Failed to connect: $e');
    }
  }

  void _onServerMessage(dynamic message) {
    // Check if the message is binary (audio chunk)
    if (message is List<int>) {
      if (_isReceivingAudio) {
        _audioBuffer.addAll(message);
      }
      return;
    }

    // Otherwise, assume it's a string message (JSON)
    if (message is String) {
      try {
        final decoded = jsonDecode(message) as Map<String, dynamic>;

        // Handle Audio Start Marker
        if (decoded['type'] == 'audio_start') {
          _isReceivingAudio = true;
          _audioBuffer.clear();
          return;
        }

        // Handle Audio End Marker
        if (decoded['type'] == 'audio_end') {
          _isReceivingAudio = false;
          _playBufferedAudio();
          return;
        }

        // Handle text reply or error
        _handleTextResponse(decoded);
      } catch (e) {
        debugPrint('Failed to parse text message: $e');
        _handleTextResponse({'reply': message}); // Fallback to raw string
      }
    }
  }
  
  void _handleTextResponse(Map<String, dynamic> decoded) {
    _responseStopwatch?.stop();
    final elapsed = _responseStopwatch?.elapsed ?? Duration.zero;
    
    String replyText;
    if (decoded.containsKey('error')) {
      replyText = 'Error: ${decoded['error']}';
    } else {
      replyText = (decoded['reply'] as String?)?.trim().isNotEmpty == true
          ? decoded['reply']!
          : '(no reply)';
    }

    setState(() {
      // Replace the last "loading" assistant bubble with the real reply
      final loadingIndex = _messages.lastIndexWhere((m) => m.isLoading);
      if (loadingIndex != -1) {
        _messages[loadingIndex] = _messages[loadingIndex].copyWith(
          text: replyText,
          isLoading: false,
          responseTime: elapsed,
        );
      } else {
        _messages.add(ChatMessage(
          isUser: false,
          text: replyText,
          responseTime: elapsed,
        ));
      }
      _waitingForReply = false;
    });

    _scrollToBottom();
  }
  
  Future<void> _playBufferedAudio() async {
    if (_audioBuffer.isEmpty) return;
    
    try {
      final bytes = Uint8List.fromList(_audioBuffer);
      // Play the MP3 bytes using audioplayers
      await _audioPlayer.play(BytesSource(bytes));
    } catch (e) {
      debugPrint("Error playing audio: $e");
      _showSnack("Failed to play response audio");
    }
  }

  Future<void> _startRecording() async {
    if (!_isConnected) {
      _showSnack('Not connected to server');
      _connect();
      return;
    }
    if (_waitingForReply) {
      _showSnack('Waiting for previous response...');
      return;
    }

    final hasPermission = await _requestMicPermission();
    if (!hasPermission || !await _recorder.hasPermission()) {
      _showSnack('Microphone permission denied');
      return;
    }
    
    // Stop any currently playing audio when starting a new recording
    await _audioPlayer.stop();

    try {
      const config = RecordConfig(
        encoder: AudioEncoder.pcm16bits,
        sampleRate: 16000,
        numChannels: 1,
      );

      final stream = await _recorder.startStream(config);

      setState(() {
        _isRecording = true;
        _recordingDuration = Duration.zero;
      });

      _recordingTimer = Timer.periodic(const Duration(milliseconds: 100), (_) {
        setState(() => _recordingDuration += const Duration(milliseconds: 100));
      });

      _audioStreamSubscription = stream.listen(
        (chunk) {
          _channel?.sink.add(chunk); // raw PCM16 bytes
        },
        onError: (error) {
          debugPrint('Audio stream error: $error');
          _showSnack('Audio error: $error');
          _stopRecording();
        },
      );
    } catch (e) {
      debugPrint('Failed to start recording: $e');
      _showSnack('Failed to start recording: $e');
    }
  }

  Future<void> _stopRecording() async {
    _recordingTimer?.cancel();
    _recordingTimer = null;

    await _audioStreamSubscription?.cancel();
    _audioStreamSubscription = null;

    if (await _recorder.isRecording()) {
      await _recorder.stop();
    }

    final duration = _recordingDuration;
    setState(() => _isRecording = false);

    if (duration.inMilliseconds < 300) {
      // Too short — likely an accidental tap, don't send
      return;
    }

    // Signal end-of-audio to the server (keeps the connection open)
    _channel?.sink.add('END');

    // Add the user's "voice message" bubble
    setState(() {
      _messages.add(ChatMessage(
        isUser: true,
        text: '🎤 Voice message (${_formatDuration(duration)})',
      ));
      // Add a loading bubble for the assistant's reply
      _messages.add(ChatMessage(
        isUser: false,
        text: 'Thinking...',
        isLoading: true,
      ));
      _waitingForReply = true;
    });

    _responseStopwatch = Stopwatch()..start();
    _scrollToBottom();
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 250),
          curve: Curves.easeOut,
        );
      }
    });
  }

  void _showSnack(String msg) {
    if (mounted) {
      ScaffoldMessenger.of(context).showSnackBar(SnackBar(content: Text(msg)));
    }
  }

  String _formatDuration(Duration d) {
    final seconds = d.inMilliseconds / 1000;
    return '${seconds.toStringAsFixed(1)}s';
  }

  String _formatResponseTime(Duration d) {
    final seconds = d.inMilliseconds / 1000;
    return '⏱ ${seconds.toStringAsFixed(2)}s';
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Kuyili Voice Assistant'),
        actions: [
          Padding(
            padding: const EdgeInsets.only(right: 12),
            child: Center(
              child: Row(
                children: [
                  Icon(
                    _isConnected ? Icons.wifi : Icons.wifi_off,
                    size: 18,
                    color: _isConnected ? Colors.greenAccent : Colors.redAccent,
                  ),
                  const SizedBox(width: 4),
                  Text(
                    _isConnected ? 'Connected' : 'Offline',
                    style: const TextStyle(fontSize: 12),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
      body: SafeArea(
        child: Column(
          children: [
            Expanded(
              child: _messages.isEmpty
                  ? const Center(
                      child: Text(
                        'Tap the mic and start talking',
                        style: TextStyle(color: Colors.grey),
                      ),
                    )
                  : ListView.builder(
                      controller: _scrollController,
                      padding: const EdgeInsets.all(12),
                      itemCount: _messages.length,
                      itemBuilder: (context, index) => _buildBubble(_messages[index]),
                    ),
            ),
            _buildRecordingBar(),
          ],
        ),
      )
    );
  }

  Widget _buildBubble(ChatMessage msg) {
    final isUser = msg.isUser;
    return Align(
      alignment: isUser ? Alignment.centerRight : Alignment.centerLeft,
      child: Container(
        constraints: BoxConstraints(
          maxWidth: MediaQuery.of(context).size.width * 0.75,
        ),
        margin: const EdgeInsets.symmetric(vertical: 6),
        padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
        decoration: BoxDecoration(
          color: isUser
              ? Theme.of(context).colorScheme.primary
              : Colors.grey.shade200,
          borderRadius: BorderRadius.only(
            topLeft: const Radius.circular(16),
            topRight: const Radius.circular(16),
            bottomLeft: Radius.circular(isUser ? 16 : 4),
            bottomRight: Radius.circular(isUser ? 4 : 16),
          ),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            if (msg.isLoading)
              Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  const SizedBox(
                    width: 12,
                    height: 12,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  ),
                  const SizedBox(width: 8),
                  Text(
                    msg.text,
                    style: const TextStyle(
                      fontStyle: FontStyle.italic,
                      color: Colors.black54,
                    ),
                  ),
                ],
              )
            else
              Text(
                msg.text,
                style: TextStyle(
                  color: isUser ? Colors.white : Colors.black87,
                ),
              ),
            if (msg.responseTime != null) ...[
              const SizedBox(height: 4),
              Text(
                _formatResponseTime(msg.responseTime!),
                style: TextStyle(
                  fontSize: 11,
                  color: isUser ? Colors.white70 : Colors.black45,
                ),
              ),
            ],
          ],
        ),
      ),
    );
  }

  Widget _buildRecordingBar() {
    return Container(
      padding: const EdgeInsets.symmetric(vertical: 16, horizontal: 20),
      decoration: BoxDecoration(
        color: Theme.of(context).scaffoldBackgroundColor,
        boxShadow: [
          BoxShadow(
            color: Colors.black.withOpacity(0.05),
            blurRadius: 8,
            offset: const Offset(0, -2),
          ),
        ],
      ),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          if (_isRecording)
            Padding(
              padding: const EdgeInsets.only(right: 16),
              child: Text(
                _formatDuration(_recordingDuration),
                style: const TextStyle(
                  fontSize: 16,
                  fontFeatures: [FontFeature.tabularFigures()],
                ),
              ),
            ),
          GestureDetector(
            onTap: _isRecording ? _stopRecording : _startRecording,
            child: AnimatedContainer(
              duration: const Duration(milliseconds: 200),
              width: 68,
              height: 68,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                color: _isRecording ? Colors.red : Theme.of(context).colorScheme.primary,
                boxShadow: _isRecording
                    ? [
                        BoxShadow(
                          color: Colors.red.withOpacity(0.4),
                          blurRadius: 12,
                          spreadRadius: 2,
                        ),
                      ]
                    : [],
              ),
              child: Icon(
                _isRecording ? Icons.stop : Icons.mic,
                color: Colors.white,
                size: 30,
              ),
            ),
          ),
        ],
      ),
    );
  }
}