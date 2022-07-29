/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.logs;

import java.util.Locale;

public class DataStreamNamingUtils {

    public static final String DATA_STREAM = "data_stream";
    public static final String DATA_STREAM_TYPE = DATA_STREAM + ".type";
    public static final String DATA_STREAM_DATASET = DATA_STREAM + ".dataset";
    public static final String DATA_STREAM_NAMESPACE = DATA_STREAM + ".namespace";

    private static final char[] DISALLOWED_IN_DATASET = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':', '-' };
    private static final char[] DISALLOWED_IN_NAMESPACE = new char[] { '\\', '/', '*', '?', '\"', '<', '>', '|', ' ', ',', '#', ':' };
    private static final int MAX_LENGTH = 100;
    private static final char REPLACEMENT_CHAR = '_';

    public static String sanitizeDataStreamDataset(String dataset) {
        return sanitizeDataStreamField(dataset, DISALLOWED_IN_DATASET);
    }

    public static String sanitizeDataStreamNamespace(String namespace) {
        return sanitizeDataStreamField(namespace, DISALLOWED_IN_NAMESPACE);
    }

    private static String sanitizeDataStreamField(String s, char[] disallowedInDataset) {
        if (s == null) {
            return null;
        }
        s = s.toLowerCase(Locale.ROOT);
        s = s.substring(0, Math.min(s.length(), MAX_LENGTH));
        for (char c : disallowedInDataset) {
            s = s.replace(c, REPLACEMENT_CHAR);
        }
        return s;
    }
}
