/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

final class Fixtures {

    static boolean testAgainstRemoteClusterOnly() {
        String suite = System.getProperty("tests.rest.suite");
        return "remote_cluster".equals(suite);
    }

    static String convertQueryToRemoteIndex(String query) {
        String[] commands = query.split("\\|");
        String first = commands[0];
        if (commands[0].toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("\\[");
            assert parts.length >= 1 : parts;
            String fromStatement = parts[0];
            String[] localIndices = fromStatement.substring("FROM ".length()).split(",");
            String remoteIndices = Arrays.stream(localIndices)
                .map(index -> "*:" + index.trim() + "," + index.trim())
                .collect(Collectors.joining(","));
            parts[0] = "FROM " + remoteIndices;
            return String.join(" [", parts).trim() + " " + query.substring(first.length());
        } else {
            return query;
        }
    }

    static boolean hasEnrich(String query) {
        String[] commands = query.split("\\|");
        for (int i = 0; i < commands.length; i++) {
            commands[i] = commands[i].trim();
            if (commands[i].toLowerCase(Locale.ROOT).startsWith("enrich")) {
                return true;
            }
        }
        return false;
    }

    static boolean hasIndexMetadata(String query) {
        String[] commands = query.split("\\|");
        if (commands[0].trim().toLowerCase(Locale.ROOT).startsWith("from")) {
            String[] parts = commands[0].split("\\[");
            return parts.length > 1 && parts[1].contains("_index");
        }
        return false;
    }

}
