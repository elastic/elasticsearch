/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import org.elasticsearch.tools.java_version_checker.SuppressForbidden;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.fusesource.jansi.AnsiPrintStream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * Prints the text lines, that are coming through the standard input, to the standard output
 * (like the no-arg `cat` command), but followed by a multi-line text "banner" that persists as the last text
 * that is printed to the terminal.
 * The "banner" persists because it is printed out after every line of text, and is then cleared
 * before the next line is printed. Clearing and moving the cursor only works if the output is a terminal
 * (this redirection should not be used otherwise).
 * The banner is read from an input file, and mustn't necessarily be available before the stdin input is.
 * Once the content of the banner becomes available it cannot be changed.
 */
final class OutputBottomBanner {

    @SuppressForbidden(reason = "System#out")
    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("expected two arguments, but provided " + Arrays.toString(args));
        }
        // TODO validate args
        final String bannerEndMarker = args[0];
        final String bannerFileName = args[1];
        final AtomicReference<Banner> bannerReference = new AtomicReference<>();
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

        // forward stdin to stdout
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()))) {
            String line;
            // no banner yet, forward input line by line
            while (bannerReference.get() == null) {
                // avoid blocking if no input, because banner can become available and it must be shown even if no input lines
                if (reader.ready()) {
                    // this might block if only part of line is available
                    // this is reasonable because the assumption is that the input is a stream of lines
                    // so it only blocks for a short while
                    line = reader.readLine();
                    System.out.printf(Locale.ROOT, "%s%n", line);
                } else {
                    // avoid busy looping when no input lines and no banner
                    Thread.sleep(1000);
                }
            }
            Banner banner = bannerReference.get();
            // print banner
            //System.out.printf(Locale.ROOT, "%s%n", banner.banner);
            //AnsiConsole.out().println(ansi().saveCursorPosition());
            //System.out.printf(Locale.ROOT, "%s", ansi().saveCursorPosition().toString());
            AnsiConsole.out().printf(Locale.ROOT, "%s", ansi().bold().a(banner.getBannerText()).boldOff().newline());
            //System.out.printf(Locale.ROOT, "%s", ansi().bold().a(banner.banner).boldOff().newline().toString());
            // we can block indefinitely for input lines since the banner has already been printed
            while ((line = reader.readLine()) != null) {
                // clear banner
                //System.out.printf(Locale.ROOT, "%s", ansi().cursorUpLine(banner.lineCount + 1).eraseScreen(Ansi.Erase.FORWARD));
                //System.out.printf(Locale.ROOT, "\u001b[%dF\u001b[J", banner.lineCount);
                //AnsiConsole.out().println(ansi().restoreCursorPosition());
                AnsiConsole.out().printf(Locale.ROOT, "%s",
                        ansi().cursorUpLine(banner.getLineCount(AnsiConsole.getTerminalWidth())).eraseScreen(Ansi.Erase.FORWARD));
                //System.out.printf(Locale.ROOT, "%s", ansi().restoreCursorPosition().toString());
                // line overwrites banner
                AnsiConsole.out().printf(Locale.ROOT, "%s", ansi().a(line).newline());
                //System.out.printf(Locale.ROOT, "%s%n", line);
                // append another banner
                //AnsiConsole.out().println(ansi().saveCursorPosition());
                //AnsiConsole.out().println(ansi().bold().a(banner.banner).boldOff());
                //AnsiConsole.out().printf(Locale.ROOT, "%s", ansi().bold().a(banner.banner).boldOff().newline());
                AnsiConsole.out().printf(Locale.ROOT, "%s", ansi().bold().a(banner.getBannerText()).boldOff().newline());
                //System.out.printf(Locale.ROOT, "%s", ansi().bold().a(banner.banner).boldOff().newline().toString());
                //System.out.printf(Locale.ROOT, "%s%n", banner.banner);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class Banner {
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