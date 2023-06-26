/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.search.lookup.SearchLookup;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Script producing IP addresses. Unlike the other {@linkplain AbstractFieldScript}s
 * which deal with their native java objects this converts its values to the same format
 * that Lucene uses to store its fields, {@link InetAddressPoint}. There are a few compelling
 * reasons to do this:
 * <ul>
 * <li>{@link Inet4Address}es and {@link Inet6Address} are not comparable with one another.
 * That is correct in some contexts, but not for our queries. Our queries must consider the
 * IPv4 address equal to the address that it maps to in IPv6 <a href="https://tools.ietf.org/html/rfc4291">rfc4291</a>).
 * <li>{@link InetAddress}es are not ordered, but we need to implement range queries with
 * same same ordering as {@link IpFieldMapper}. That also uses {@link InetAddressPoint}
 * so it saves us a lot of trouble to use the same representation.
 * </ul>
 */
public abstract class IpFieldScript extends AbstractFieldScript {
    public static final ScriptContext<Factory> CONTEXT = newContext("ip_field", Factory.class);

    public static final Factory PARSE_FROM_SOURCE = new Factory() {
        @Override
        public LeafFactory newFactory(String field, Map<String, Object> params, SearchLookup lookup, OnScriptError onScriptError) {
            return ctx -> new IpFieldScript(field, params, lookup, OnScriptError.FAIL, ctx) {
                @Override
                public void execute() {
                    emitFromSource();
                }
            };
        }

        @Override
        public boolean isResultDeterministic() {
            return true;
        }
    };

    public static Factory leafAdapter(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentFactory) {
        return (leafFieldName, params, searchLookup, onScriptError) -> {
            CompositeFieldScript.LeafFactory parentLeafFactory = parentFactory.apply(searchLookup);
            return (LeafFactory) ctx -> {
                CompositeFieldScript compositeFieldScript = parentLeafFactory.newInstance(ctx);
                return new IpFieldScript(leafFieldName, params, searchLookup, onScriptError, ctx) {
                    @Override
                    public void setDocument(int docId) {
                        compositeFieldScript.setDocument(docId);
                    }

                    @Override
                    public void execute() {
                        emitFromCompositeScript(compositeFieldScript);
                    }
                };
            };
        };
    }

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup, OnScriptError onScriptError);
    }

    public interface LeafFactory {
        IpFieldScript newInstance(LeafReaderContext ctx);
    }

    private BytesRef[] values = new BytesRef[1];
    private int count;

    public IpFieldScript(
        String fieldName,
        Map<String, Object> params,
        SearchLookup searchLookup,
        OnScriptError onScriptError,
        LeafReaderContext ctx
    ) {
        super(fieldName, params, searchLookup, ctx, onScriptError);
    }

    @Override
    protected void prepareExecute() {
        count = 0;
    }

    public final void runForDoc(int docId, Consumer<InetAddress> consumer) {
        runForDoc(docId);
        for (int i = 0; i < count; i++) {
            consumer.accept(InetAddressPoint.decode(values[i].bytes));
        }
    }

    /**
     * Values from the last time {@link #runForDoc(int)} was called. This array
     * is mutable and will change with the next call of {@link #runForDoc(int)}.
     * It is also oversized and will contain garbage at all indices at and
     * above {@link #count()}.
     * <p>
     * All values are IPv6 addresses so they are 16 bytes. IPv4 addresses are
     * encoded by <a href="https://tools.ietf.org/html/rfc4291">rfc4291</a>.
     */
    public final BytesRef[] values() {
        return values;
    }

    /**
     * The number of results produced the last time {@link #runForDoc(int)} was called.
     */
    public final int count() {
        return count;
    }

    @Override
    protected void emitFromObject(Object v) {
        if (v instanceof String) {
            try {
                emit((String) v);
            } catch (Exception e) {
                // ignore parsing exceptions
            }
        }
    }

    public final void emit(String v) {
        if (values.length < count + 1) {
            values = ArrayUtil.grow(values, count + 1);
        }
        BytesRef encoded = new BytesRef(InetAddressPoint.encode(InetAddresses.forString(v)));
        // encode the address and increment the count on separate lines, to ensure that
        // we don't increment if the address is badly formed
        values[count++] = encoded;
    }

    public static class Emit {
        private final IpFieldScript script;

        public Emit(IpFieldScript script) {
            this.script = script;
        }

        public void emit(String v) {
            script.checkMaxSize(script.count());
            script.emit(v);
        }
    }
}
