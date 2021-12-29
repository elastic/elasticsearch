/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.attachment;

import org.elasticsearch.test.ESTestCase;

public class TikaImplTests extends ESTestCase {

    public void testTikaLoads() throws Exception {
        Class.forName("org.elasticsearch.ingest.attachment.TikaImpl");
    }

}
