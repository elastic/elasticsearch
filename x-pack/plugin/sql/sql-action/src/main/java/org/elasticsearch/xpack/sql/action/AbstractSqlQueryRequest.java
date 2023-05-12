/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.Version.CURRENT;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseFieldsValue;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE_ARRAY;
import static org.elasticsearch.xpack.sql.action.ProtoShim.fromProto;
import static org.elasticsearch.xpack.sql.action.ProtoShim.toProto;
import static org.elasticsearch.xpack.sql.action.Protocol.CATALOG_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.CLIENT_ID_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.CURSOR_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.FETCH_SIZE_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.FILTER_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.MODE_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.PAGE_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.PARAMS_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.QUERY_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.REQUEST_TIMEOUT_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.TIME_ZONE_NAME;
import static org.elasticsearch.xpack.sql.action.Protocol.VERSION_NAME;

/**
 * Base class for requests that contain sql queries (Query and Translate)
 */
public abstract class AbstractSqlQueryRequest extends AbstractSqlRequest implements CompositeIndicesRequest {

    //
    // parser for sql-proto SqlTypedParamValue
    //
    private static final ConstructingObjectParser<SqlTypedParamValue, Void> SQL_PARAM_PARSER = new ConstructingObjectParser<>(
        "params",
        true,
        objects -> new SqlTypedParamValue((String) objects[1], objects[0])
    );

    private static final ParseField VALUE = new ParseField("value");
    private static final ParseField TYPE = new ParseField("type");

    static {
        SQL_PARAM_PARSER.declareField(constructorArg(), (p, c) -> parseFieldsValue(p), VALUE, ValueType.VALUE);
        SQL_PARAM_PARSER.declareString(constructorArg(), TYPE);
    }

    private String query = "";
    private ZoneId zoneId = Protocol.TIME_ZONE;
    private String catalog = null;
    private int fetchSize = Protocol.FETCH_SIZE;
    private TimeValue requestTimeout = Protocol.REQUEST_TIMEOUT;
    private TimeValue pageTimeout = Protocol.PAGE_TIMEOUT;
    @Nullable
    private QueryBuilder filter = null;
    private List<SqlTypedParamValue> params = emptyList();
    private Map<String, Object> runtimeMappings = emptyMap();

    static final ParseField QUERY = new ParseField(QUERY_NAME);
    static final ParseField CURSOR = new ParseField(CURSOR_NAME);
    static final ParseField PARAMS = new ParseField(PARAMS_NAME);
    static final ParseField TIME_ZONE = new ParseField(TIME_ZONE_NAME);
    static final ParseField CATALOG = new ParseField(CATALOG_NAME);
    static final ParseField FETCH_SIZE = new ParseField(FETCH_SIZE_NAME);
    static final ParseField REQUEST_TIMEOUT = new ParseField(REQUEST_TIMEOUT_NAME);
    static final ParseField PAGE_TIMEOUT = new ParseField(PAGE_TIMEOUT_NAME);
    static final ParseField FILTER = new ParseField(FILTER_NAME);
    static final ParseField MODE = new ParseField(MODE_NAME);
    static final ParseField CLIENT_ID = new ParseField(CLIENT_ID_NAME);
    static final ParseField VERSION = new ParseField(VERSION_NAME);

    public AbstractSqlQueryRequest() {
        super();
    }

    public AbstractSqlQueryRequest(
        String query,
        List<SqlTypedParamValue> params,
        QueryBuilder filter,
        Map<String, Object> runtimeMappings,
        ZoneId zoneId,
        String catalog,
        int fetchSize,
        TimeValue requestTimeout,
        TimeValue pageTimeout,
        RequestInfo requestInfo
    ) {
        super(requestInfo);
        this.query = query;
        this.params = params;
        this.zoneId = zoneId;
        this.catalog = catalog;
        this.fetchSize = fetchSize;
        this.requestTimeout = requestTimeout;
        this.pageTimeout = pageTimeout;
        this.filter = filter;
        this.runtimeMappings = runtimeMappings;
    }

    protected static <R extends AbstractSqlQueryRequest> ObjectParser<R, Void> objectParser(Supplier<R> supplier) {
        // Using an ObjectParser here (vs. ConstructingObjectParser) because the latter needs to instantiate a concrete class
        // and we would duplicate the code from this class to its subclasses
        ObjectParser<R, Void> parser = new ObjectParser<>("sql/query", false, supplier);
        parser.declareString(AbstractSqlQueryRequest::query, QUERY);
        parser.declareString((request, mode) -> request.mode(Mode.fromString(mode)), MODE);
        parser.declareString(AbstractSqlRequest::clientId, CLIENT_ID);
        parser.declareString(AbstractSqlRequest::version, VERSION);
        parser.declareField(AbstractSqlQueryRequest::params, AbstractSqlQueryRequest::parseParams, PARAMS, VALUE_ARRAY);
        parser.declareString((request, zoneId) -> request.zoneId(ZoneId.of(zoneId)), TIME_ZONE);
        parser.declareString(AbstractSqlQueryRequest::catalog, CATALOG);
        parser.declareInt(AbstractSqlQueryRequest::fetchSize, FETCH_SIZE);
        parser.declareString(
            (request, timeout) -> request.requestTimeout(TimeValue.parseTimeValue(timeout, Protocol.REQUEST_TIMEOUT, REQUEST_TIMEOUT_NAME)),
            REQUEST_TIMEOUT
        );
        parser.declareString(
            (request, timeout) -> request.pageTimeout(TimeValue.parseTimeValue(timeout, Protocol.PAGE_TIMEOUT, PAGE_TIMEOUT_NAME)),
            PAGE_TIMEOUT
        );
        parser.declareObject(AbstractSqlQueryRequest::filter, (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p), FILTER);
        parser.declareObject(AbstractSqlQueryRequest::runtimeMappings, (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
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
                    currentParam = SQL_PARAM_PARSER.apply(p, null);
                    /*
                     * Always set the xcontentlocation for the first param just in case the first one happens to not meet the parsing rules
                     * that are checked later in validateParams method.
                     * Also, set the xcontentlocation of the param that is different from the previous param in list when it comes to
                     * its type being explicitly set or inferred.
                     */
                    if ((previousParam != null && previousParam.hasExplicitType() == false) || result.isEmpty()) {
                        currentParam.tokenLocation(toProto(loc));
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
                        currentParam.tokenLocation(toProto(loc));
                    }
                }

                result.add(currentParam);
                previousParam = currentParam;
            }
        }

        return result;
    }

    protected static void validateParams(List<SqlTypedParamValue> params, Mode mode) {
        for (SqlTypedParamValue param : params) {
            if (Mode.isDriver(mode) && param.hasExplicitType() == false) {
                throw new XContentParseException(
                    fromProto(param.tokenLocation()),
                    "[params] must be an array where each entry is an object with a " + "value/type pair"
                );
            }
            if (Mode.isDriver(mode) == false && param.hasExplicitType()) {
                throw new XContentParseException(
                    fromProto(param.tokenLocation()),
                    "[params] must be an array where each entry is a single field (no " + "objects supported)"
                );
            }
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        // the version field is mandatory for drivers and CLI
        Mode mode = requestInfo().mode();
        if (Mode.isDedicatedClient(mode)) {
            if (requestInfo().version() == null) {
                if (Strings.hasText(query())) {
                    validationException = addValidationError(
                        "[version] is required for the [" + mode.toString() + "] client",
                        validationException
                    );
                }
            } else if (SqlVersion.isClientCompatible(SqlVersion.fromId(CURRENT.id), requestInfo().version()) == false) {
                validationException = addValidationError(
                    "The ["
                        + requestInfo().version()
                        + "] version of the ["
                        + mode.toString()
                        + "] "
                        + "client is not compatible with Elasticsearch version ["
                        + CURRENT
                        + "]",
                    validationException
                );
            }
        }
        if (runtimeMappings != null) {
            // Perform a superficial check for runtime_mappings structure. The full check cannot happen until the actual search request
            validationException = validateRuntimeMappings(runtimeMappings, validationException);
        }
        return validationException;
    }

    private static ActionRequestValidationException validateRuntimeMappings(
        Map<String, Object> runtimeMappings,
        ActionRequestValidationException validationException
    ) {
        for (Map.Entry<String, Object> entry : runtimeMappings.entrySet()) {
            // top level objects are fields
            String fieldName = entry.getKey();
            if (entry.getValue() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propNode = (Map<String, Object>) entry.getValue();
                if (propNode.get("type") == null) {
                    return addValidationError("No type specified for runtime field [" + fieldName + "]", validationException);
                }
            } else {
                return addValidationError(
                    "Expected map for runtime field [" + fieldName + "] definition but got [" + fieldName.getClass().getSimpleName() + "]",
                    validationException
                );
            }
        }
        return validationException;
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

    public String catalog() {
        return catalog;
    }

    public AbstractSqlQueryRequest catalog(String catalog) {
        this.catalog = catalog;
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

    public Map<String, Object> runtimeMappings() {
        return runtimeMappings;
    }

    /**
     * An optional list of runtime fields that can be added to the request similar to the way it is done in Query DSL search requests
     */
    public AbstractSqlQueryRequest runtimeMappings(Map<String, Object> runtimeMappings) {
        this.runtimeMappings = runtimeMappings;
        return this;
    }

    public AbstractSqlQueryRequest(StreamInput in) throws IOException {
        super(in);
        query = in.readString();
        params = in.readList(AbstractSqlQueryRequest::readSqlTypedParamValue);
        zoneId = in.readZoneId();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0)) {
            catalog = in.readOptionalString();
        }
        fetchSize = in.readVInt();
        requestTimeout = in.readTimeValue();
        pageTimeout = in.readTimeValue();
        filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_13_0)) {
            runtimeMappings = in.readMap();
        }
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
        out.writeCollection(params, AbstractSqlQueryRequest::writeSqlTypedParamValue);
        out.writeZoneId(zoneId);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0)) {
            out.writeOptionalString(catalog);
        }
        out.writeVInt(fetchSize);
        out.writeTimeValue(requestTimeout);
        out.writeTimeValue(pageTimeout);
        out.writeOptionalNamedWriteable(filter);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_13_0)) {
            out.writeGenericMap(runtimeMappings);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        AbstractSqlQueryRequest that = (AbstractSqlQueryRequest) o;
        return fetchSize == that.fetchSize
            && Objects.equals(query, that.query)
            && Objects.equals(params, that.params)
            && Objects.equals(zoneId, that.zoneId)
            && Objects.equals(catalog, that.catalog)
            && Objects.equals(requestTimeout, that.requestTimeout)
            && Objects.equals(pageTimeout, that.pageTimeout)
            && Objects.equals(filter, that.filter)
            && Objects.equals(runtimeMappings, that.runtimeMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            query,
            params,
            zoneId,
            catalog,
            fetchSize,
            requestTimeout,
            pageTimeout,
            filter,
            runtimeMappings
        );
    }
}
