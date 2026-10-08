/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.common;

public class CharCodes {
    // an optimization to store byte codes for different character types instead of enum references, to make it more cache-friendly
    public static final byte DIGIT_CHAR_CODE = 0;
    public static final byte ALPHABETIC_CHAR_CODE = 1;
    public static final byte SUBTOKEN_DELIMITER_CHAR_CODE = 2;
    public static final byte TOKEN_DELIMITER_CHAR_CODE = 3;
    public static final byte TOKEN_BOUNDARY_CHAR_CODE = 4;
    public static final byte LINE_END_CODE = 5;
    public static final byte OTHER_CHAR_CODE = 6;
}
