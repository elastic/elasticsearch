/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IpScriptFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.CompositeFieldScript;
import org.elasticsearch.script.IpFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.field.IpDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.runtime.IpScriptFieldExistsQuery;
import org.elasticsearch.search.runtime.IpScriptFieldRangeQuery;
import org.elasticsearch.search.runtime.IpScriptFieldTermQuery;
import org.elasticsearch.search.runtime.IpScriptFieldTermsQuery;

import java.net.InetAddress;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class IpScriptFieldType extends AbstractScriptFieldType<IpFieldScript.LeafFactory> {

    public static final RuntimeField.Parser PARSER = new RuntimeField.Parser(name -> new Builder<>(name, IpFieldScript.CONTEXT) {
        @Override
        AbstractScriptFieldType<?> createFieldType(String name, IpFieldScript.Factory factory, Script script, Map<String, String> meta) {
            return new IpScriptFieldType(name, factory, getScript(), meta());
        }

        @Override
        IpFieldScript.Factory getParseFromSourceFactory() {
            return IpFieldScript.PARSE_FROM_SOURCE;
        }

        @Override
        IpFieldScript.Factory getCompositeLeafFactory(Function<SearchLookup, CompositeFieldScript.LeafFactory> parentScriptFactory) {
            return IpFieldScript.leafAdapter(parentScriptFactory);
        }
    });

    IpScriptFieldType(String name, IpFieldScript.Factory scriptFactory, Script script, Map<String, String> meta) {
        super(
            name,
            searchLookup -> scriptFactory.newFactory(name, script.getParams(), searchLookup),
            script,
            scriptFactory.isResultDeterministic(),
            meta
        );
    }

    @Override
    public String typeName() {
        return IpFieldMapper.CONTENT_TYPE;
    }

    @Override
    public Object valueForDisplay(Object value) {
        if (value == null) {
            return null;
        }
        return DocValueFormat.IP.format((BytesRef) value);
    }

    @Override
    public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
        checkNoFormat(format);
        checkNoTimeZone(timeZone);
        return DocValueFormat.IP;
    }

    @Override
    public IpScriptFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
        return new IpScriptFieldData.Builder(name(), leafFactory(fieldDataContext.lookupSupplier().get()), IpDocValuesField::new);
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        applyScriptContext(context);
        return new IpScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        SearchExecutionContext context
    ) {
        applyScriptContext(context);
        return IpFieldMapper.IpFieldType.rangeQuery(
            lowerTerm,
            upperTerm,
            includeLower,
            includeUpper,
            (lower, upper) -> new IpScriptFieldRangeQuery(
                script,
                leafFactory(context),
                name(),
                new BytesRef(InetAddressPoint.encode(lower)),
                new BytesRef(InetAddressPoint.encode(upper))
            )
        );
    }

    @Override
    public Query termQuery(Object value, SearchExecutionContext context) {
        applyScriptContext(context);
        if (value instanceof InetAddress) {
            return inetAddressQuery((InetAddress) value, context);
        }
        String term = BytesRefs.toString(value);
        if (term.contains("/")) {
            return cidrQuery(term, context);
        }
        InetAddress address = InetAddresses.forString(term);
        return inetAddressQuery(address, context);
    }

    private Query inetAddressQuery(InetAddress address, SearchExecutionContext context) {
        return new IpScriptFieldTermQuery(script, leafFactory(context), name(), new BytesRef(InetAddressPoint.encode(address)));
    }

    @Override
    public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
        applyScriptContext(context);
        BytesRefHash terms = new BytesRefHash(values.size(), BigArrays.NON_RECYCLING_INSTANCE);
        List<Query> cidrQueries = null;
        for (Object value : values) {
            if (value instanceof InetAddress) {
                terms.add(new BytesRef(InetAddressPoint.encode((InetAddress) value)));
                continue;
            }
            String term = BytesRefs.toString(value);
            if (false == term.contains("/")) {
                terms.add(new BytesRef(InetAddressPoint.encode(InetAddresses.forString(term))));
                continue;
            }
            if (cidrQueries == null) {
                cidrQueries = new ArrayList<>();
            }
            cidrQueries.add(cidrQuery(term, context));
        }
        Query termsQuery = new IpScriptFieldTermsQuery(script, leafFactory(context), name(), terms);
        if (cidrQueries == null) {
            return termsQuery;
        }
        BooleanQuery.Builder bool = new BooleanQuery.Builder();
        bool.add(termsQuery, Occur.SHOULD);
        for (Query cidrQuery : cidrQueries) {
            bool.add(cidrQuery, Occur.SHOULD);
        }
        return bool.build();
    }

    private Query cidrQuery(String term, SearchExecutionContext context) {
        Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(term);
        InetAddress addr = cidr.v1();
        int prefixLength = cidr.v2();
        // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
        byte lower[] = addr.getAddress();
        byte upper[] = addr.getAddress();
        for (int i = prefixLength; i < 8 * lower.length; i++) {
            int m = 1 << (7 - (i & 7));
            lower[i >> 3] &= ~m;
            upper[i >> 3] |= m;
        }
        // Force the terms into IPv6
        BytesRef lowerBytes = new BytesRef(InetAddressPoint.encode(InetAddressPoint.decode(lower)));
        BytesRef upperBytes = new BytesRef(InetAddressPoint.encode(InetAddressPoint.decode(upper)));
        return new IpScriptFieldRangeQuery(script, leafFactory(context), name(), lowerBytes, upperBytes);
    }
}
