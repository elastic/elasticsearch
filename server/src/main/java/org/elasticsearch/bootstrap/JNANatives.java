/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.nativeaccess.NativeAccess;

/**
 * This class performs the actual work with JNA and library bindings to call native methods. It should only be used after
 * we are sure that the JNA classes are available to the JVM
 */
class JNANatives {

    /** no instantiation */
    private JNANatives() {}

    private static final Logger logger = LogManager.getLogger(JNANatives.class);

    static String rlimitToString(long value) {
        assert Constants.LINUX || Constants.MAC_OS_X;
        if (value == NativeAccess.instance().getRlimitInfinity()) {
            return "unlimited";
        } else {
            return Long.toUnsignedString(value);
        }
    }

}
