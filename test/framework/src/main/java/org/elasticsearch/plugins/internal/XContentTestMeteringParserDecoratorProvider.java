/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ServiceLoader;

/**
 * A testing only SPI provider to exercise XContent tests with different implementations
 * of {@link XContentMeteringParserDecorator}.
 */
public interface XContentTestMeteringParserDecoratorProvider {
    XContentMeteringParserDecorator get();

    /** Decorate the {@link XContentParser} with a decorator if available on the classpath. */
    static XContentParser decorateParser(XContentParser parser) {
        return Holder.INSTANCE.decorate(parser, Mapping.EMPTY);
    }

    static XContentMeteringParserDecorator instance() {
        return Holder.INSTANCE;
    }

    final class Holder {
        private Holder() {

        }

        private static final XContentMeteringParserDecorator INSTANCE = loadXContentMeteringParserDecorator();

        private static XContentMeteringParserDecorator loadXContentMeteringParserDecorator() {
            var providers = ServiceLoader.load(XContentTestMeteringParserDecoratorProvider.class).iterator();
            if (providers.hasNext() == false) {
                return XContentMeteringParserDecorator.NOOP;
            }
            var provider = providers.next().get();
            assert providers.hasNext() == false : "Only a single XContentTestMeteringParserDecoratorProvider is supported.";
            return provider;
        }
    }
}
