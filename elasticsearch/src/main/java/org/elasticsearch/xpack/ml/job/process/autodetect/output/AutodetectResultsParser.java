/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.output;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * Parses the JSON output of the autodetect program.
 * <p>
 * Expects an array of buckets so the first element will always be the
 * start array symbol and the data must be terminated with the end array symbol.
 */
public class AutodetectResultsParser extends AbstractComponent {

    public AutodetectResultsParser(Settings settings) {
        super(settings);
    }

    public Stream<AutodetectResult> parseResults(InputStream in) throws ElasticsearchParseException {
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY, in);
            XContentParser.Token token = parser.nextToken();
            // if start of an array ignore it, we expect an array of buckets
            if (token != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("unexpected token [" + token + "]");
            }
            Spliterator<AutodetectResult> spliterator = Spliterators.spliterator(new AutodetectResultIterator(parser), Long.MAX_VALUE, 0);
            return StreamSupport.stream(spliterator, false)
                    .onClose(() -> consumeAndCloseStream(in));
        } catch (IOException e) {
            consumeAndCloseStream(in);
            throw new ElasticsearchParseException(e.getMessage(), e);
        }
    }

    private void consumeAndCloseStream(InputStream in) {
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
            logger.warn("Error closing result parser input stream", e);
        }
    }

    private class AutodetectResultIterator implements Iterator<AutodetectResult> {

        private final XContentParser parser;
        private XContentParser.Token token;

        private AutodetectResultIterator(XContentParser parser) {
            this.parser = parser;
            token = parser.currentToken();
        }

        @Override
        public boolean hasNext() {
            try {
                token = parser.nextToken();
            } catch (IOException e) {
                throw new ElasticsearchParseException(e.getMessage(), e);
            }
            if (token == XContentParser.Token.END_ARRAY) {
                return false;
            } else if (token != XContentParser.Token.START_OBJECT) {
                logger.error("Expecting Json Field name token after the Start Object token");
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

