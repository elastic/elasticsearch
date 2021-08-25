/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * Prints the text lines, that are coming through the standard input, to the standard output
 * (like the no-arg `cat` command), but followed by a multi-line text "banner" that persists as the last text
 * that is printed to the terminal.
 * The "banner" persists because it is printed out before waiting for more input on stdin.
 * Clearing and moving the cursor only works if the output is a terminal
 * (this redirection should not be used otherwise).
 * The banner is read from an input file, and mustn't necessarily be available before the stdin input is.
 * Once the content of the banner becomes available it cannot be changed (it is set-once).
 */
final class TerminalOutputFormatter {

    // generous buffer used to forward stdin to stdout, multiple log lines at a time
    private static final int BUFFER_SIZE = 32768;

    private static final int DEFAULT_BANNER_DISPLAY_LIFETIME_MILLIS = 5 * 60 * 1000;

    private final byte[] buffer;
    private int terminalWidth;
    private String clearBannerCommand;
    private String richBanner;
    private Banner lastBanner;

    // non-private for tests
    TerminalOutputFormatter(byte[] buffer) {
        this.buffer = buffer;
        this.terminalWidth = -1; // terminal width can dynamically change
        this.clearBannerCommand = null;
        this.richBanner = null;
        this.lastBanner = null;
    }

    public static void main(final String[] args) throws IOException {
        final AnsiType ansiType = AnsiConsole.out().getType();
        final boolean bannerSupported = ansiType != AnsiType.Unsupported && ansiType != AnsiType.Redirected;
        // in the no-arg mode simply check that the ANSI escape sequences are supported given the output and the OS types
        if (args.length == 0) {
            // uninstall for good measure
            AnsiConsole.systemUninstall();
            if (ansiType == AnsiType.Unsupported) {
                System.exit(1);
            } else if (ansiType == AnsiType.Redirected) {
                System.exit(2);
            } else {
                System.exit(0);
            }
        }
        // TODO check failing these validations prevents the node from starting
        // args validation is done for good measure, even if displaying the banner is not supported
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected two arguments, but provided " + Arrays.toString(args) +
                    " . The first arguments contains the text used to mark the end of the banner to be read, " +
                    "but which will not be included in the output banner. The second argument contains the file path " +
                    "from where the banner is to be read, which might or might not be available when this is run, and " +
                    "which will be output, inline with the forwarded input, as soon as available.");
        }
        final String bannerEndMarker = args[0];
        if (bannerEndMarker == null || bannerEndMarker.isEmpty() ||
                bannerEndMarker.trim().isEmpty() || bannerEndMarker.indexOf('\n') != -1) {
            throw new IllegalArgumentException("The banner end marker value must not be empty, contain only whitespaces, " +
                    "or contain any line breaks");
        }
        final String bannerInputFilePath = args[1];
        if (false == Files.isReadable(Paths.get(bannerInputFilePath))) {
            throw new IllegalArgumentException("Banner input file does not exist or is not readable");
        }
        final byte[] buffer = new byte[BUFFER_SIZE];
        // only bother with the banner if it can be actually displayed
        // ideally this code should not be invoked at all in this case because of the incurred copying overhead
        final AtomicReference<Banner> bannerReference = new AtomicReference<>();
        if (bannerSupported) {
            getAsyncBanner(bannerInputFilePath, bannerEndMarker, bannerReference);
        }
        try {
            new TerminalOutputFormatter(buffer).forward(System.in, System.out, bannerReference);
        } finally {
            // if this throws while the terminal had formatting state, the following resets that state
            AnsiConsole.systemUninstall();
        }
    }

    void forward(InputStream in, OutputStream out, AtomicReference<Banner> bannerReference) throws IOException {
        Banner banner = null; // set-once
        boolean clearBanner = false;
        boolean lineBoundary = true;
        int start = 0;
        long bannerLifetimeMillis = Long.MAX_VALUE;
        while (true) {
            // only print the banner at line boundary AND when blocking for reads from stdin
            // IOException will be propagated to terminate the program: input forwarding stops and the stacktrace prints to stderr
            if (lineBoundary && in.available() == 0) {
                // banner is set-once, when available
                if (banner == null) {
                    banner = bannerReference.get();
                    // this is the first time the banner becomes available to print
                    if (banner != null) {
                        bannerLifetimeMillis = Instant.now().toEpochMilli() + banner.getLifetimeInMillis();
                    }
                }
                // This is the simplest way to display the banner for a limited time only.
                // But it is very possible that the banner remain printed while blocking for input to forward.
                // To mitigate this we need to start off another thread and share access to the terminal banner printing routines
                // between the two threads (this adds inevitable overhead for questionable benefits).
                if (banner != null && Instant.now().toEpochMilli() < bannerLifetimeMillis) {
                    printBanner(banner);
                    clearBanner = true;
                }
            }
            assert start < buffer.length;
            // it is OK to block here because if the banner is available it would be printed
            // IOException will be propagated to terminate the program (input forwarding will be stopped)
            int bytesRead = in.read(buffer, start, buffer.length - start);
            assert bytesRead != 0;
            if (bytesRead < 0) {
                // the program should definitely exit if there is no input to forward anymore
                return;
            } else {
                int end = start + bytesRead;
                // find the last end-of-line in the newly buffered content
                int lineBreakPos = end - 1;
                while (lineBreakPos >= start && buffer[lineBreakPos] != (byte)'\n') {
                    lineBreakPos--;
                }
                // the buffered content contains at least one complete line
                if (lineBreakPos >= start) {
                    // before forwarding the input, clear the existing banner (if any)
                    if (clearBanner) {
                        assert banner != null;
                        clearBanner(banner);
                        clearBanner = false;
                    }
                    // forward input to output, until last end of line
                    out.write(buffer, 0, lineBreakPos + 1);
                    // move the remaining bytes (of an incomplete line) to the head of the buffer
                    start = end - lineBreakPos - 1;
                    System.arraycopy(buffer, lineBreakPos + 1, buffer, 0, start);
                    assert clearBanner == false;
                    // only print the banner if the next read blocks
                    lineBoundary = true;
                } else if (end >= buffer.length) {
                    // the buffer is full and it does not contain any end-of-line (the input produces lines longer than the buffer size)
                    // print the currently buffered line fragment, but do not print the banner
                    // in this case, it is possible that the read for the subsequent banner fragment blocks,
                    // while there is no banner printed
                    // this is not great but better than the alternatives
                    if (clearBanner) {
                        assert banner != null;
                        clearBanner(banner);
                        clearBanner = false;
                    }
                    out.write(buffer, 0, buffer.length);
                    start = 0;
                    assert clearBanner == false;
                    // the banner should never break the lines
                    lineBoundary = false;
                } else {
                    // no end-of-line found, all the buffered content is only an incomplete line fragment
                    // nothing to display, read on
                    start = end;
                    assert start < buffer.length;
                }
            }
        }
    }

    private void clearBanner(Banner banner) {
        Objects.nonNull(banner);
        if (terminalWidth != AnsiConsole.getTerminalWidth() || banner != lastBanner) {
            // recompute the clear banner ANSI escape sequence
            terminalWidth = AnsiConsole.getTerminalWidth();
            lastBanner = banner;
            // update the banner command to account for the changed number of lines
            clearBannerCommand =
                    ansi().cursorUpLine(banner.getLineCount(terminalWidth) + 1).eraseScreen(Ansi.Erase.FORWARD).toString();
        }
        AnsiConsole.out().print(clearBannerCommand);
    }

    private void printBanner(Banner banner) {
        Objects.nonNull(banner);
        if (banner != lastBanner) {
            lastBanner = banner;
            // cache formatted (bolded) text
            richBanner = ansi().newline().bold().a(banner.getBannerText()).boldOff().newline().toString();
        }
        AnsiConsole.out().print(richBanner);
    }

    private static void getAsyncBanner(String bannerFileName, String bannerEndMarker, AtomicReference<Banner> bannerReference) {
        // asynchronously read the whole banner; the banner is only used when complete
        final Thread bannerThread = new Thread(() -> {
            final Banner.Builder bannerBuilder = Banner.builder();
            // read banner from file using platform's charset
            try (BufferedReader reader = new BufferedReader(new FileReader(bannerFileName, StandardCharsets.UTF_8))) {
                while (true) {
                    String bannerLine;
                    while ((bannerLine = reader.readLine()) != null) {
                        if (bannerEndMarker.equals(bannerLine)) {
                            break;
                        }
                        bannerBuilder.appendLine(bannerLine);
                    }
                    // the banner is now complete (marker present on its own line)
                    if (bannerEndMarker.equals(bannerLine)) {
                        break;
                    }
                    // this is EOF without the banner end marker. Keep reading until encountering the end marker.
                    Thread.sleep(1000);
                }
                bannerReference.set(bannerBuilder.build());
            } catch (IOException e) {
                // this will crash this thread and output to stderr
                // but the stdin -> stdout continues to function
                throw new UncheckedIOException(e);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        });
        // this program can theoretically terminate before the banner ever becomes available
        bannerThread.setDaemon(true);
        bannerThread.start();
    }

    private static class Banner {

        private final String bannerText;
        private final List<Integer> lineLengths;
        private final long lifetimeInMillis;
        private final Map<Integer, Integer> lineCountCache = new ConcurrentHashMap<>(1);

        static class Builder {
            private StringBuilder stringBuilder = new StringBuilder();
            private List<Integer> lineLengths = new ArrayList<>();

            private long lifetimeInMillis = DEFAULT_BANNER_DISPLAY_LIFETIME_MILLIS;

            private Builder() {
            }

            void appendLine(String line) {
                if (false == lineLengths.isEmpty()) {
                    stringBuilder.append(System.lineSeparator());
                }
                stringBuilder.append(line);
                lineLengths.add(line.length());
            }

            void lifetime(int millis) {
                this.lifetimeInMillis = millis;
            }

            Banner build() {
                return new Banner(stringBuilder.toString(), lifetimeInMillis, lineLengths);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        private Banner(String bannerText, long lifetimeInMillis, List<Integer> lineLengths) {
            this.bannerText = bannerText;
            this.lifetimeInMillis = lifetimeInMillis;
            this.lineLengths = lineLengths;
        }

        public int getLineCount(int terminalWidth) {
            return lineCountCache.computeIfAbsent(terminalWidth, tw -> computeLineCount(terminalWidth));
        }

        public long getLifetimeInMillis() {
            return lifetimeInMillis;
        }

        public String getBannerText() {
            return bannerText;
        }

        private int computeLineCount(int terminalWidth) {
            int lineCount = 0;
            for (Integer lineLength : lineLengths) {
                lineCount += lineLength / terminalWidth;
                if (lineLength % terminalWidth != 0) {
                    lineCount++;
                }
            }
            return lineCount;
        }
    }
}