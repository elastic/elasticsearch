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

final class OutputBottomBanner {

    private static String BANNER_END_MARKER = "EOF";

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected the banner file sole argument, but provided " + Arrays.toString(args));
        }
        final AtomicReference<Banner> bannerReference = new AtomicReference<>();
        // asynchronously read the whole banner
        final Thread bannerThread = new Thread(() -> {
            StringBuilder bannerBuilder = new StringBuilder();
            int lineCount = 0;
            // read file using platform's charset
            try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
                while (true) {
                    String bannerLine;
                    while ((bannerLine = reader.readLine()) != null) {
                        if (BANNER_END_MARKER.equals(bannerLine)) {
                            break;
                        }
                        if (lineCount != 0) {
                            bannerBuilder.append(System.lineSeparator());
                        }
                        bannerBuilder.append(bannerLine);
                        lineCount++;
                    }
                    if (BANNER_END_MARKER.equals(bannerLine)) {
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

        // forward stdin
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()))) {
            String line;
            // no banner, yet
            while (bannerReference.get() == null) {
                // avoid blocking if no input, because banner can become available and it must be shown
                if (reader.ready()) {
                    line = reader.readLine();
                    System.out.printf("%s%n", line);
                } else {
                    // avoid busy looping when no input and no banner
                    Thread.sleep(1000);
                }
            }
            Banner banner = bannerReference.get();
            printBanner(banner);
            // print banner
            while ((line = reader.readLine()) != null) {
                System.out.printf("%s%n", line);
                printBanner(banner);
//                if (bannerLineCount == 1) {
//                    System.out.printf("\u001b[J%s%n%s\r", line, bannerReference);
//                } else {
//                    System.out.printf("\u001b[J%s%n%s\u001b[%dF", line, bannerReference, bannerLineCount - 1);
//                }
                //printLineWithBanner(line, banner.get());
                //if (bannerLineCount == 0) {
                //    System.out.printf("%s%n", line);
                //} else if (bannerLineCount == 1) {
                //    System.out.printf("\u001b[J%s%n%s\r", line, banner);
                //} else {
                //    System.out.printf("\u001b[J%s%n%s\u001b[%dF", line, banner, bannerLineCount - 1);
                //}
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

    private static void printBanner(Banner banner) {
        if (banner == null || banner.lineCount == 0) {
            // nothing to print
        } else if (banner.lineCount == 1) {
            // clear screen following the cursor
            // print one line banner
            // move cursor back to banner line start
            System.out.printf("\u001b[J%s\r", banner);
        } else if (banner.lineCount > 1) {
            // clear screen following the cursor
            // print multi-line banner
            // move cursor back to banner first line start
            System.out.printf("\u001b[J%s\u001b[%dF", banner.banner, banner.lineCount - 1);
        }
    }

    private static void printLineWithBanner(String line, Banner banner) {
        if (banner == null || banner.lineCount == 0) {
            System.out.printf("%s%n", line);
        } else if (banner.lineCount == 1) {
            // clear screen following the cursor
            // print line
            // print one line banner
            // move cursor back to banner line start
            System.out.printf("\u001b[J%s%n%s\r", line, banner);
        } else if (banner.lineCount > 1) {
            // clear screen following the cursor
            // print line
            // print multi-line banner
            // move cursor back to banner first line start
            System.out.printf("\u001b[J%s%n%s\u001b[%dF", line, banner.banner, banner.lineCount - 1);
        }
    }

}
