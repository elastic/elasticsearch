/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.equalTo;

public class LogConfiguratorTests extends ESTestCase {

    public void testJavaUtilLoggingFormat(){
        final ByteArrayOutputStream myOut = new ByteArrayOutputStream();
        PrintStream err = System.err;
        PrintStream out = System.out;
        try{
            System.setErr(new PrintStream(myOut));
            System.setOut(new PrintStream(myOut));


            Logger.getLogger("any").info("message");

            assertThat(myOut.toString(), equalTo("[INFO][any] message\n"));
        }finally {
            System.setOut(out);
            System.setErr(err);
        }
    }


}
