/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.Selector;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Prints lines at the tail of the console output
 */
final class OutputBottomBanner {

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
            try (BufferedReader reader = new BufferedReader(new FileReader(bannerFileName))) {
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
                    System.out.printf("%s%n", line);
                } else {
                    // avoid busy looping when no input lines and no banner
                    Thread.sleep(1000);
                }
            }
            Banner banner = bannerReference.get();
            // print banner
            System.out.printf("%s%n", banner.banner);
            // we can block indefinitely for input lines since the banner has already been printed
            while ((line = reader.readLine()) != null) {
                // clear banner
                System.out.printf("\u001b[%dF\u001b[J", banner.lineCount);
                // line overwrites banner
                System.out.printf("%s%n", line);
                // append another banner
                System.out.printf("%s%n", banner.banner);
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
