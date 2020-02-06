/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Base class for requests that contain sql queries (Query and Translate)
 */
public abstract class AbstractSqlQueryRequest extends AbstractSqlRequest implements CompositeIndicesRequest, ToXContentFragment {

    private String query = "";
    private ZoneId zoneId = Protocol.TIME_ZONE;
    private int fetchSize = Protocol.FETCH_SIZE;
    private TimeValue requestTimeout = Protocol.REQUEST_TIMEOUT;
    private TimeValue pageTimeout = Protocol.PAGE_TIMEOUT;
    @Nullable
    private QueryBuilder filter = null;
    private List<SqlTypedParamValue> params = Collections.emptyList();
    
    static final ParseField QUERY = new ParseField("query");
    static final ParseField CURSOR = new ParseField("cursor");
    static final ParseField PARAMS = new ParseField("params");
    static final ParseField TIME_ZONE = new ParseField("time_zone");
    static final ParseField FETCH_SIZE = new ParseField("fetch_size");
    static final ParseField REQUEST_TIMEOUT = new ParseField("request_timeout");
    static final ParseField PAGE_TIMEOUT = new ParseField("page_timeout");
    static final ParseField FILTER = new ParseField("filter");
    static final ParseField MODE = new ParseField("mode");
    static final ParseField CLIENT_ID = new ParseField("client_id");

    public AbstractSqlQueryRequest() {
        super();
    }

    public AbstractSqlQueryRequest(String query, List<SqlTypedParamValue> params, QueryBuilder filter, ZoneId zoneId,
            int fetchSize, TimeValue requestTimeout, TimeValue pageTimeout, RequestInfo requestInfo) {
        super(requestInfo);
        this.query = query;
        this.params = params;
        this.zoneId = zoneId;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
    }

    protected static <R extends AbstractSqlQueryRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        // Using an ObjectParser here (vs. ConstructingObjectParser) because the latter needs to instantiate a concrete class
        // and we would duplicate the code from this class to its subclasses
        ObjectParser<R, Void> parser = new ObjectParser<>("sql/query", false, supplier);
        parser.declareString(AbstractSqlQueryRequest::query, QUERY);
        parser.declareString((request, mode) -> request.mode(Mode.fromString(mode)), MODE);
        parser.declareString((request, clientId) -> request.clientId(clientId), CLIENT_ID);
        parser.declareField(AbstractSqlQueryRequest::params, AbstractSqlQueryRequest::parseParams, PARAMS, ValueType.VALUE_ARRAY);
        parser.declareString((request, zoneId) -> request.zoneId(ZoneId.of(zoneId)), TIME_ZONE);
        parser.declareInt(AbstractSqlQueryRequest::fetchSize, FETCH_SIZE);
        parser.declareString((request, timeout) -> request.requestTimeout(TimeValue.parseTimeValue(timeout, Protocol.REQUEST_TIMEOUT,
                "request_timeout")), REQUEST_TIMEOUT);
        parser.declareString(
                (request, timeout) -> request.pageTimeout(TimeValue.parseTimeValue(timeout, Protocol.PAGE_TIMEOUT, "page_timeout")),
                PAGE_TIMEOUT);
        parser.declareObject(AbstractSqlQueryRequest::filter,
                (p, c) -> AbstractQueryBuilder.parseInnerQueryBuilder(p), FILTER);
        return parser;
    }

    /**
     * Text of SQL query
     */
    public String query() {
        return query;
    }

    public AbstractSqlQueryRequest query(String query) {
        if (query == null) {
            throw new IllegalArgumentException("query may not be null.");
        }
        this.query = query;
        return this;
    }

    /**
     * An optional list of parameters if the SQL query is parametrized
     */
    public List<SqlTypedParamValue> params() {
        return params;
    }

    public AbstractSqlQueryRequest params(List<SqlTypedParamValue> params) {
        if (params == null) {
            throw new IllegalArgumentException("params may not be null.");
        }
        this.params = params;
        return this;
    }
    
    private static List<SqlTypedParamValue> parseParams(XContentParser p) throws IOException {
        List<SqlTypedParamValue> result = new ArrayList<>();
        Token token = p.currentToken();
        
        if (token == Token.START_ARRAY) {
            Object value = null;
            String type = null;
            SqlTypedParamValue previousParam = null;
            SqlTypedParamValue currentParam = null;
            
            while ((token = p.nextToken()) != Token.END_ARRAY) {
                XContentLocation loc = p.getTokenLocation();
                
                if (token == Token.START_OBJECT) {
                    // we are at the start of a value/type pair... hopefully
                    currentParam = SqlTypedParamValue.fromXContent(p);
                    /*
                     * Always set the xcontentlocation for the first param just in case the first one happens to not meet the parsing rules
                     * that are checked later in validateParams method.
                     * Also, set the xcontentlocation of the param that is different from the previous param in list when it comes to 
                     * its type being explicitly set or inferred.
                     */
                    if ((previousParam != null && previousParam.hasExplicitType() == false) || result.isEmpty()) {
                        currentParam.tokenLocation(loc);
                    }
                } else {
                    if (token == Token.VALUE_STRING) {
                        value = p.text();
                        type = "keyword";
                    } else if (token == Token.VALUE_NUMBER) {
                        XContentParser.NumberType numberType = p.numberType();
                        if (numberType == XContentParser.NumberType.INT) {
                            value = p.intValue();
                            type = "integer";
                        } else if (numberType == XContentParser.NumberType.LONG) {
                            value = p.longValue();
                            type = "long";
                        } else if (numberType == XContentParser.NumberType.FLOAT) {
                            value = p.floatValue();
                            type = "float";
                        } else if (numberType == XContentParser.NumberType.DOUBLE) {
                            value = p.doubleValue();
                            type = "double";
                        }
                    } else if (token == Token.VALUE_BOOLEAN) {
                        value = p.booleanValue();
                        type = "boolean";
                    } else if (token == Token.VALUE_NULL) {
                        value = null;
                        type = "null";
                    } else {
                        throw new XContentParseException(loc, "Failed to parse object: unexpected token [" + token + "] found");
                    }
                    
                    currentParam = new SqlTypedParamValue(type, value, false);
                    if ((previousParam != null && previousParam.hasExplicitType()) || result.isEmpty()) {
                        currentParam.tokenLocation(loc);
                    }
                }

                result.add(currentParam);
                previousParam = currentParam;
            }
        }
        
        return result;
    }
    
    protected static void validateParams(List<SqlTypedParamValue> params, Mode mode) {
        for(SqlTypedParamValue param : params) {            
            if (Mode.isDriver(mode) && param.hasExplicitType() == false) {
                throw new XContentParseException(param.tokenLocation(), "[params] must be an array where each entry is an object with a "
                        + "value/type pair");
            }
            if (Mode.isDriver(mode) == false && param.hasExplicitType()) {
                throw new XContentParseException(param.tokenLocation(), "[params] must be an array where each entry is a single field (no "
                        + "objects supported)");
            }
        }
    }

    /**
     * The client's time zone
     */
    public ZoneId zoneId() {
        return zoneId;
    }

    public AbstractSqlQueryRequest zoneId(ZoneId zoneId) {
        if (zoneId == null) {
            throw new IllegalArgumentException("time zone may not be null.");
        }
        this.zoneId = zoneId;
        return this;
    }

    /**
     * Hint about how many results to fetch at once.
     */
    public int fetchSize() {
        return fetchSize;
    }

    /**
     * Hint about how many results to fetch at once.
     */
    public AbstractSqlQueryRequest fetchSize(int fetchSize) {
        if (fetchSize <= 0) {
            throw new IllegalArgumentException("fetch_size must be more than 0.");
        }
        this.fetchSize = fetchSize;
        return this;
    }

    /**
     * The timeout specified on the search request
     */
    public TimeValue requestTimeout() {
        return requestTimeout;
    }

    public AbstractSqlQueryRequest requestTimeout(TimeValue requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * The scroll timeout
     */
    public TimeValue pageTimeout() {
        return pageTimeout;
    }

    public AbstractSqlQueryRequest pageTimeout(TimeValue pageTimeout) {
        this.pageTimeout = pageTimeout;
        return this;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public AbstractSqlQueryRequest filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    /**
     * An optional Query DSL defined query that can added as a filter on the top of the SQL query
     */
    public QueryBuilder filter() {
        return filter;
    }

    public AbstractSqlQueryRequest(StreamInput in) throws IOException {
        super(in);
        query = in.readString();
        params = in.readList(AbstractSqlQueryRequest::readSqlTypedParamValue);
        zoneId = in.readZoneId();
        fetchSize = in.readVInt();
        requestTimeout = in.readTimeValue();
        pageTimeout = in.readTimeValue();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public static void writeSqlTypedParamValue(StreamOutput out, SqlTypedParamValue value) throws IOException {
        out.writeString(value.type);
        out.writeGenericValue(value.value);
        out.writeBoolean(value.hasExplicitType());
    }

    public static SqlTypedParamValue readSqlTypedParamValue(StreamInput in) throws IOException {
        return new SqlTypedParamValue(in.readString(), in.readGenericValue(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(query);
        out.writeVInt(params.size());
        for (SqlTypedParamValue param: params) {
            writeSqlTypedParamValue(out, param);
        }
        out.writeZoneId(zoneId);
        out.writeVInt(fetchSize);
        out.writeTimeValue(requestTimeout);
        out.writeTimeValue(pageTimeout);
        out.writeOptionalNamedWriteable(filter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AbstractSqlQueryRequest that = (AbstractSqlQueryRequest) o;
        return fetchSize == that.fetchSize &&
                Objects.equals(query, that.query) &&
                Objects.equals(params, that.params) &&
                Objects.equals(zoneId, that.zoneId) &&
                Objects.equals(requestTimeout, that.requestTimeout) &&
                Objects.equals(pageTimeout, that.pageTimeout) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, params, zoneId, fetchSize, requestTimeout, pageTimeout, filter);
    }
}
