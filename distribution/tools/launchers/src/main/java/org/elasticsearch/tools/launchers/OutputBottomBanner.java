/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import org.elasticsearch.tools.java_version_checker.SuppressForbidden;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

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
            StringBuilder bannerBuilder = new StringBuilder();
            int lineCount = 0;
            // read banner from file using platform's charset
            try (BufferedReader reader = new BufferedReader(new FileReader(bannerFileName, Charset.defaultCharset()))) {
                while (true) {
                    String bannerLine;
                    while ((bannerLine = reader.readLine()) != null) {
                        if (bannerEndMarker.equals(bannerLine)) {
                            break;
                        }
                        if (lineCount != 0) {
                            bannerBuilder.append(System.lineSeparator());
                        }
                        bannerBuilder.append(bannerLine);
                        lineCount++;
                    }
                    // the banner is now complete (marker on its own line)
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
            bannerReference.set(new Banner(bannerBuilder.toString(), lineCount));
        });
        bannerThread.setDaemon(true);
        bannerThread.start();

        // forward stdin to stdout
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()))) {
            String line;
            // no banner yet
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
            System.out.printf(Locale.ROOT, "%s%n", banner.banner);
            // we can block indefinitely for input lines since the banner has already been printed
            while ((line = reader.readLine()) != null) {
                // clear banner
                System.out.printf(Locale.ROOT, "\u001b[%dF\u001b[J", banner.lineCount);
                // line overwrites banner
                System.out.printf(Locale.ROOT, "%s%n", line);
                // append another banner
                System.out.printf(Locale.ROOT, "%s%n", banner.banner);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class Banner {
        String banner;
        Integer lineCount;

        Banner(String banner, Integer lineCount) {
            this.banner = banner;
            this.lineCount = lineCount;
        }
    }

}
