/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.fielddata.IpScriptFieldData;
import org.elasticsearch.xpack.runtimefields.query.IpScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.IpScriptFieldRangeQuery;
import org.elasticsearch.xpack.runtimefields.query.IpScriptFieldTermQuery;
import org.elasticsearch.xpack.runtimefields.query.IpScriptFieldTermsQuery;

import java.net.InetAddress;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

public final class IpScriptMappedFieldType extends AbstractScriptMappedFieldType<IpFieldScript.LeafFactory> {
    IpScriptMappedFieldType(String name, Script script, IpFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, scriptFactory::newFactory, meta);
    }

    @Override
    protected String runtimeType() {
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
        if (format != null) {
            String message = "Field [%s] of type [%s] with runtime type [%s] does not support custom formats";
            throw new IllegalArgumentException(String.format(Locale.ROOT, message, name(), typeName(), runtimeType()));
        }
        if (timeZone != null) {
            String message = "Field [%s] of type [%s] with runtime type [%s] does not support custom time zones";
            throw new IllegalArgumentException(String.format(Locale.ROOT, message, name(), typeName(), runtimeType()));
        }
        return DocValueFormat.IP;
    }

    @Override
    public IpScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new IpScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
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
        QueryShardContext context
    ) {
        checkAllowExpensiveQueries(context);
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
    public Query termQuery(Object value, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
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

    private Query inetAddressQuery(InetAddress address, QueryShardContext context) {
        return new IpScriptFieldTermQuery(script, leafFactory(context), name(), new BytesRef(InetAddressPoint.encode(address)));
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        checkAllowExpensiveQueries(context);
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

    private Query cidrQuery(String term, QueryShardContext context) {
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
