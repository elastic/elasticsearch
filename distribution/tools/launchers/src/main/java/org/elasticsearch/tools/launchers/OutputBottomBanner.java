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
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

final class OutputBottomBanner {

    public static void main(final String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected single argument but was " + Arrays.toString(args));
        }
        StringBuilder bannerBuilder = new StringBuilder();
//        try (FileInputStream fis = new FileInputStream(new File(args[0]))) {
//            banner.append(new String(fis.readAllBytes(), Charset.defaultCharset()));
//        }
        int bannerLineCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
            String bannerLine;
            while ((bannerLine = reader.readLine()) != null) {
                if (bannerLineCount != 0) {
                    bannerBuilder.append(System.lineSeparator());
                }
                bannerBuilder.append(bannerLine);
                bannerLineCount++;
            }
        }
        String banner = bannerBuilder.toString();
        System.out.println("THIS IS BANNER");
        System.out.println(banner);
        System.out.println("THAT WAS BANNER " + bannerLineCount);
//        String banner = bannerLines.stream().reduce()
        // strip trailing line ends
//        while (banner.length() > 0 && (banner.charAt(banner.length() - 1) == '\r' ||
//                banner.charAt(banner.length() - 1) == '\n')) {
//            banner = banner.substring(0, banner.length() - 1);
//        }
        // TODO check charset and console
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, Charset.defaultCharset()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (bannerLineCount == 0) {
                    System.out.printf("%s%n", line);
                } else if (bannerLineCount == 1) {
                    System.out.printf("\u001b[J%s%n%s\r", line, banner);
                } else {
                    System.out.printf("\u001b[J%s%n%s\u001b[%dF", line, banner, bannerLineCount - 1);
                }
//                System.out.printf("%s%n", line);
//                System.out.printf("\u001b[s"); // save cursor position
//                for (String bannerLine : bannerLines) {
//                    System.out.printf("%s%n", bannerLine);
//                }
//                System.out.printf("\u001b[u"); // restore cursor position
//                System.out.printf("\u001b[" + (bannerLines.size() + 1) + "A");
            }
        }
    }

}
