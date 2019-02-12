/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

/**
 * POSIX exit codes.
 */
public class ExitCodes {
    public static final int OK = 0;
    public static final int USAGE = 64;          /* command line usage error */
    public static final int DATA_ERROR = 65;     /* data format error */
    public static final int NO_INPUT = 66;       /* cannot open input */
    public static final int NO_USER = 67;        /* addressee unknown */
    public static final int NO_HOST = 68;        /* host name unknown */
    public static final int UNAVAILABLE = 69;    /* service unavailable */
    public static final int CODE_ERROR = 70;     /* internal software error */
    public static final int CANT_CREATE = 73;    /* can't create (user) output file */
    public static final int IO_ERROR = 74;       /* input/output error */
    public static final int TEMP_FAILURE = 75;   /* temp failure; user is invited to retry */
    public static final int PROTOCOL = 76;       /* remote error in protocol */
    public static final int NOPERM = 77;         /* permission denied */
    public static final int CONFIG = 78;         /* configuration error */

    private ExitCodes() { /* no instance, just constants */ }
}
