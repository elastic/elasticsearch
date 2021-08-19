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
final class OutputBottomBanner {

    // generous buffer used to forward stdin to stdout, multiple log lines at a time
    private static final int BUFFER_SIZE = 16384;
    private static final byte[] BUFFER = new byte[BUFFER_SIZE];

    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("expected two arguments, but provided " + Arrays.toString(args));
        }
        // TODO validate args
        // TODO timer arg for limited lifetime banner
        final String bannerEndMarker = args[0];
        final String bannerFileName = args[1];
        final AtomicReference<Banner> bannerReference = new AtomicReference<>();
        // don't bother with the banner text if the output is not a terminal
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
            // TODO consider logging
            bannerReference.set(bannerBuilder.build());
        });
        bannerThread.setDaemon(true);
        bannerThread.start();

        String clearBannerCommand = null;
        int terminalWidth = -1;
        boolean clearBanner = false;
        Banner banner = null; // set-once
        String richBanner = null;
        boolean lineBoundary = true; // banner printed only if the previous text ended with an end-of-line
        while (true) {
            if (lineBoundary) {
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
                    clearBanner = true;
                }
            }
            // the banner is (maybe) printed atm, we can block for reads now
            int bytesRead = System.in.read(BUFFER, 0, BUFFER.length);
            if (bytesRead < 0) {
                // the program should definitely exit if there is no input to forward anymore
                return;
            } else if (bytesRead == 0) { // should never return "0", but just in case it does, loop
                continue;
            } else {
                // before forwarding input, clear the banner
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
                // forward input to output
                System.out.write(BUFFER, 0, bytesRead);
                // it is expected that the piped input is a stream of lines
                // if the input only returns line fragments, then the banner never gets
                lineBoundary = BUFFER[bytesRead - 1] == (byte)'\n';
                for (int i = bytesRead - 1; i >= 0; i++) {

                }
            }
        }
        // TODO what happens if I terminate the program or interrupt the main thread
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