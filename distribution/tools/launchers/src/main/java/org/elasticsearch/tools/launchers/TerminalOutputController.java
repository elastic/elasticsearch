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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
// TODO rename to ES node terminal output (verify that ES and logs still work)
final class TerminalOutputController {

    // generous buffer used to forward stdin to stdout, multiple log lines at a time
    private static final int BUFFER_SIZE = 16384;
    private static final byte[] BUFFER = new byte[BUFFER_SIZE];

    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("expected two arguments, but provided " + Arrays.toString(args));
        }
        // TODO validate args
        // TODO timer arg for limited lifetime banner
        // TODO use JANSI to double check that output is terminal (and that it supports cmds)
        // TODO think about how program can terminate abnormally
        final String bannerEndMarker = args[0];
        final String bannerFileName = args[1];
        final AtomicReference<Banner> bannerReference = new AtomicReference<>();
        getBannerTextAsync(bannerFileName, bannerEndMarker, bannerReference);

        String clearBannerCommand = null;
        int terminalWidth = -1;
        boolean clearBanner = false;
        Banner banner = null; // set-once
        String richBanner = null;
        boolean printBanner = true; // banner printed only if the previous text ended with an end-of-line
        int start = 0;
        while (true) {
            if (printBanner) {
                // banner is set-once
                if (banner == null) {
                    banner = bannerReference.get();
                    if (banner != null) {
                        // cache formatted (bolded) text
                        richBanner = ansi().newline().bold().a(banner.getBannerText()).boldOff().newline().toString();
                    }
                }
                if (richBanner != null) {
                    AnsiConsole.out().print(richBanner);
                    // TODO print banner lifetime
                    clearBanner = true;
                }
            }
            if (start >= BUFFER.length) {
                throw new IllegalStateException("Line longer [" + start + "] than buffer size");
            }
            // the banner is (maybe) printed atm, we can block for reads now
            int bytesRead = System.in.read(BUFFER, start, BUFFER.length - start);
            if (bytesRead == 0) {
                throw new IllegalStateException("read call should always attempt to read at least one byte");
            }
            if (bytesRead < 0) {
                // the program should definitely exit if there is no input to forward anymore
                return;
            } else {
                int end = start + bytesRead;
                int lineBreakPos = end - 1;
                while (lineBreakPos >= start && BUFFER[lineBreakPos] != (byte)'\n') {
                    lineBreakPos--;
                }
                // before forwarding input, clear the banner
                if (lineBreakPos >= start) {
                    if (clearBanner) {
                        assert banner != null;
                        if (terminalWidth != AnsiConsole.getTerminalWidth()) {
                            terminalWidth = AnsiConsole.getTerminalWidth();
                            clearBannerCommand =
                                    ansi().cursorUpLine(banner.getLineCount(terminalWidth) + 1).eraseScreen(Ansi.Erase.FORWARD).toString();
                        }
                        AnsiConsole.out().print(clearBannerCommand);
                        clearBanner = false;
                    }
                    // forward input to output, until last end of line
                    System.out.write(BUFFER, 0, lineBreakPos + 1);
                    start = end - lineBreakPos - 1;
                    System.arraycopy(BUFFER, lineBreakPos + 1, BUFFER, 0, start);
                    printBanner = true;
                } else {
                    start = end;
                    printBanner = false;
                }
            }
        }
    }

    private static void getBannerTextAsync(String bannerFileName, String bannerEndMarker, AtomicReference<Banner> bannerReference) {
        // asynchronously read the whole banner; the banner is only used when complete
        final Thread bannerThread = new Thread(() -> {
            Banner.Builder bannerBuilder = Banner.builder();
            // read banner from file using platform's charset
            try (BufferedReader reader = new BufferedReader(new FileReader(bannerFileName, Charset.defaultCharset()))) {
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
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
            bannerReference.set(bannerBuilder.build());
        });
        bannerThread.setDaemon(true);
        bannerThread.start();
    }

    private static class Banner {

        private static final int MAX_BANNER_LENGTH = 8192; // TODO banner can have unlimited length?

        private final String bannerText;
        private final List<Integer> lineLengths;
        private final Map<Integer, Integer> lineCountCache = new ConcurrentHashMap<>(1);

        static class Builder {
            private StringBuilder stringBuilder = new StringBuilder();
            private List<Integer> lineLengths = new ArrayList<>();

            private Builder() {
            }

            void appendLine(String line) {
                if (false == lineLengths.isEmpty()) {
                    stringBuilder.append(System.lineSeparator());
                }
                stringBuilder.append(line);
                if (stringBuilder.length() > MAX_BANNER_LENGTH) {
                    throw new IllegalArgumentException("Read banner length exceeds limit [" + MAX_BANNER_LENGTH + "]");
                }
                lineLengths.add(line.length());
            }

            Banner build() {
                return new Banner(stringBuilder.toString(), lineLengths);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        private Banner(String bannerText, List<Integer> lineLengths) {
            this.bannerText = bannerText;
            this.lineLengths = lineLengths;
        }

        public int getLineCount(int terminalWidth) {
            return lineCountCache.computeIfAbsent(terminalWidth, tw -> computeLineCount(terminalWidth));
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