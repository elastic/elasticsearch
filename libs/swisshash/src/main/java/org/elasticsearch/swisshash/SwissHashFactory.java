/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Optional;

public abstract class SwissHashFactory {

    private static final Logger logger = LogManager.getLogger(SwissHashFactory.class);

    private static final SwissHashFactory INSTANCE;

    private SwissHashFactory() {}

    public static SwissHashFactory getInstance() {
        return INSTANCE;
    }

    public abstract LongSwissHash newLongSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker);

    public abstract BytesRefSwissHash newBytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays);

    private static final class SwissHashFactoryImpl extends SwissHashFactory {
        @Override
        public LongSwissHash newLongSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker) {
            return new LongSwissHash(recycler, breaker);
        }

        @Override
        public BytesRefSwissHash newBytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays) {
            return new BytesRefSwissHash(recycler, breaker, bigArrays);
        }
    }

    static {
        var vecMod = lookupVectorModule();
        if (vecMod.isPresent()) {
            SwissHashFactory.class.getModule().addReads(vecMod.get());
            INSTANCE = new SwissHashFactoryImpl();
        } else {
            logger.warn(
                "Java vector incubator module is not readable. "
                    + "For optimal vector performance, pass '--add-modules jdk.incubator.vector' to enable Vector API."
            );
            INSTANCE = null;
        }
    }

    private static Optional<Module> lookupVectorModule() {
        return Optional.ofNullable(SwissHash.class.getModule().getLayer()).orElse(ModuleLayer.boot()).findModule("jdk.incubator.vector");
    }
}
