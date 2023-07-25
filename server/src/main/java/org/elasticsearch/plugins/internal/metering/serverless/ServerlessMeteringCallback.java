/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal.metering.serverless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.plugins.internal.metering.MeteringCallback;
import org.elasticsearch.xcontent.XContentParser;

public class ServerlessMeteringCallback implements MeteringCallback {

    private final Logger logger = LogManager.getLogger(ServerlessMeteringCallback.class);

    @Override
    public XContentParser wrapParser(XContentParser parser) {
        return new MeteringParser(parser);
    }

    @Override
    public void reportDocumentParsed(XContentParser context) {
        // in serverless it should always be CountingDocumentParserContext
        assert context instanceof MeteringParser;
        MeteringParser counting = (MeteringParser) context;
        logger.info("REPORTING " + counting.getCounter());
        // reportManager.report(index,counter)
    }

}
