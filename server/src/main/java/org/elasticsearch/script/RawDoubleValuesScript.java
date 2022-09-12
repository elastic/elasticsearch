/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.SortField;

import java.util.function.Function;

/**
 * A script compiled into it's raw executable form
 * <p>
 * The raw executable script can be used by other scripting engines as a
 * base for their internal execution. This class provides the means for building
 * scripting engines on top of other scripting engines.
 */
public abstract class RawDoubleValuesScript {

    public RawDoubleValuesScript() {}

    public abstract double evaluate(DoubleValues[] functionValues);

    public abstract DoubleValuesSource getDoubleValuesSource(Function<String, DoubleValuesSource> sourceProvider);

    public abstract SortField getSortField(Function<String, DoubleValuesSource> sourceProvider, boolean reverse);

    public abstract Rescorer getRescorer(Function<String, DoubleValuesSource> sourceProvider);

    public abstract String sourceText();

    public abstract String[] variables();

    /** A factory to construct {@link RawDoubleValuesScript} instances. */
    public interface Factory extends ScriptFactory {
        RawDoubleValuesScript newInstance();
    }

    @SuppressWarnings("rawtypes")
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("raw_double_values", Factory.class);
}
