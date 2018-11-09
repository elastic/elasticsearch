/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;


/**
 * Parses the JSON output of the autodetect program.
 * <p>
 * Expects an array of buckets so the first element will always be the
 * start array symbol and the data must be terminated with the end array symbol.
 */
public class AutodetectResultsParser extends AbstractComponent {
    public Iterator<AutodetectResult> parseResults(InputStream in) throws ElasticsearchParseException {
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, in);
            XContentParser.Token token = parser.nextToken();
            // if start of an array ignore it, we expect an array of results
            if (token != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("unexpected token [" + token + "]");
            }
            return new AutodetectResultIterator(in, parser);
        } catch (IOException e) {
            consumeAndCloseStream(in);
            throw new ElasticsearchParseException(e.getMessage(), e);
        }
    }

    static void consumeAndCloseStream(InputStream in) {
        try {
            // read anything left in the stream before
            // closing the stream otherwise if the process
            // tries to write more after the close it gets
            // a SIGPIPE
            byte[] buff = new byte[512];
            while (in.read(buff) >= 0) {
                // Do nothing
            }
            in.close();
        } catch (IOException e) {
            throw new RuntimeException("Error closing result parser input stream", e);
        }
    }

    private class AutodetectResultIterator implements Iterator<AutodetectResult> {

        private final InputStream in;
        private final XContentParser parser;
        private XContentParser.Token token;

        private AutodetectResultIterator(InputStream in, XContentParser parser) {
            this.in = in;
            this.parser = parser;
            token = parser.currentToken();
        }

        @Override
        public boolean hasNext() {
            try {
                token = parser.nextToken();
            } catch (IOException e) {
                logger.debug("io error while parsing", e);
                consumeAndCloseStream(in);
                return false;
            }
            if (token == XContentParser.Token.END_ARRAY) {
                consumeAndCloseStream(in);
                return false;
            } else if (token != XContentParser.Token.START_OBJECT) {
                logger.error("Expecting Json Field name token after the Start Object token");
                consumeAndCloseStream(in);
                throw new ElasticsearchParseException("unexpected token [" + token + "]");
            }
            return true;
        }

        @Override
        public AutodetectResult next() {
            return AutodetectResult.PARSER.apply(parser, null);
        }
    }
}

