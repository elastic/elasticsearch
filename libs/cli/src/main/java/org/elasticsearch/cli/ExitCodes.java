/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli;

/**
 * POSIX exit codes.
 */
public class ExitCodes {
    public static final int OK = 0;
    public static final int NOOP = 63;           // nothing to do
    public static final int USAGE = 64;          // command line usage error
    public static final int DATA_ERROR = 65;     // data format error
    public static final int NO_INPUT = 66;       // cannot open input
    public static final int NO_USER = 67;        // addressee unknown
    public static final int NO_HOST = 68;        // host name unknown
    public static final int UNAVAILABLE = 69;    // service unavailable
    public static final int CODE_ERROR = 70;     // internal software error
    public static final int CANT_CREATE = 73;    // can't create (user) output file
    public static final int IO_ERROR = 74;       // input/output error
    public static final int TEMP_FAILURE = 75;   // temp failure; user is invited to retry
    public static final int PROTOCOL = 76;       // remote error in protocol
    public static final int NOPERM = 77;         // permission denied
    public static final int CONFIG = 78;         // configuration error

    private ExitCodes() { /* no instance, just constants */ }
}
