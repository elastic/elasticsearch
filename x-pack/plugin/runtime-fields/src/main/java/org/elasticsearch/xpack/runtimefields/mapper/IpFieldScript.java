/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

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
    public static final ScriptContext<Factory> CONTEXT = newContext("ip_script_field", Factory.class);

    static List<Whitelist> whitelist() {
        return List.of(WhitelistLoader.loadFromResourceFiles(RuntimeFieldsPainlessExtension.class, "ip_whitelist.txt"));
    }

    @SuppressWarnings("unused")
    public static final String[] PARAMETERS = {};

    public interface Factory extends ScriptFactory {
        LeafFactory newFactory(String fieldName, Map<String, Object> params, SearchLookup searchLookup);
    }

    public interface LeafFactory {
        IpFieldScript newInstance(LeafReaderContext ctx);
    }

    public static final Factory PARSE_FROM_SOURCE = (field, params, lookup) -> (LeafFactory) ctx -> new IpFieldScript(
        field,
        params,
        lookup,
        ctx
    ) {
        @Override
        public void execute() {
            for (Object v : extractFromSource(field)) {
                if (v instanceof String) {
                    try {
                        emit((String) v);
                    } catch (Exception e) {
                        // ignore parsing exceptions
                    }
                }
            }
        }
    };

    private BytesRef[] values = new BytesRef[1];
    private int count;

    public IpFieldScript(String fieldName, Map<String, Object> params, SearchLookup searchLookup, LeafReaderContext ctx) {
        super(fieldName, params, searchLookup, ctx);
    }

    /**
     * Execute the script for the provided {@code docId}.
     */
    public final void runForDoc(int docId) {
        count = 0;
        setDocument(docId);
        execute();
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

    protected final void emit(String v) {
        checkMaxSize(count);
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
            script.emit(v);
        }
    }
}
