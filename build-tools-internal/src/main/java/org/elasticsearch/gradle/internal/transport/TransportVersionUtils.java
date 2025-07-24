/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import com.google.common.collect.Comparators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class TransportVersionUtils {

    public record TransportVersionSetData(String name, List<Integer> ids) {
    }

    public static TransportVersionSetData readDataFile(File file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            String[] parts = reader.readLine().replaceAll("\\s+", "").split("\\|");
            if (parts.length < 2) {
                throw new IllegalStateException("Invalid transport version file format, "
                    + "must have both name and id(s): [" + file.getAbsolutePath() + "]");
            }
            if (reader.readLine() != null) {
                throw new IllegalStateException("Invalid transport version file format, "
                    + "data file must contain only one line: [" + file.getAbsolutePath() + "]");
            }
            String name = parts[0];
            var ids = Arrays.stream(parts).skip(1).map(Integer::parseInt).toList();
            var areIdsSorted = Comparators.isInOrder(ids, Comparator.naturalOrder());
            if (areIdsSorted == false) {
                throw new IllegalStateException("invalid transport version file format, "
                    + "ids are not in order: [" + file.getAbsolutePath() + "], ");
            }
            return new TransportVersionSetData(name, ids);
        } catch (IOException ioe) {
            throw new UncheckedIOException("cannot parse transport version [" + file.getAbsolutePath() + "]", ioe);
        }
    }
}
