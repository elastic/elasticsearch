/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli;

/**
 * Standard POSIX exit codes for command-line tools.
 *
 * <p>These exit codes follow POSIX conventions and are used by CLI commands
 * to indicate the result of their execution. These values are part of the public
 * API and may be used in scripts, so they should not be changed.
 *
 * <p><b>Warning:</b> Do not modify these values as they may be used in external scripts
 * where usages are not tracked by the IDE.
 */
public class ExitCodes {
    /** Successful completion (exit code 0). */
    public static final int OK = 0;

    /** Command line usage error (exit code 64). */
    public static final int USAGE = 64;

    /** Data format error (exit code 65). */
    public static final int DATA_ERROR = 65;

    /** Cannot open input (exit code 66). */
    public static final int NO_INPUT = 66;

    /** Addressee unknown (exit code 67). */
    public static final int NO_USER = 67;

    /** Host name unknown (exit code 68). */
    public static final int NO_HOST = 68;

    /** Service unavailable (exit code 69). */
    public static final int UNAVAILABLE = 69;

    /** Internal software error (exit code 70). */
    public static final int CODE_ERROR = 70;

    /** Can't create (user) output file (exit code 73). */
    public static final int CANT_CREATE = 73;

    /** Input/output error (exit code 74). */
    public static final int IO_ERROR = 74;

    /** Temporary failure; user is invited to retry (exit code 75). */
    public static final int TEMP_FAILURE = 75;

    /** Remote error in protocol (exit code 76). */
    public static final int PROTOCOL = 76;

    /** Permission denied (exit code 77). */
    public static final int NOPERM = 77;

    /** Configuration error (exit code 78). */
    public static final int CONFIG = 78;

    /** Nothing to do (exit code 80). */
    public static final int NOOP = 80;

    private ExitCodes() { /* no instance, just constants */ }
}
