/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public interface Template extends ToXContent {

    String render(Map<String, Object> model);

    interface Parser<T extends Template> {

        T parse(XContentParser parser) throws IOException, ParseException;

        class ParseException extends WatcherException {

            public ParseException(String msg) {
                super(msg);
            }

            public ParseException(String msg, Throwable cause) {
                super(msg, cause);
            }
        }
    }

    interface SourceBuilder extends ToXContent {
    }

    class InstanceSourceBuilder implements SourceBuilder {

        private final Template template;

        public InstanceSourceBuilder(Template template) {
            this.template = template;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return template.toXContent(builder, params);
        }
    }
}
