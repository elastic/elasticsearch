/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Metadata;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ingest metadata available to user code.
 *
 * The Metadata values in {@link IngestDocument.Metadata} are validated when updated.
 * _index, _id and _routing must be a String or null
 * _version_type must be a lower case VersionType or null
 * _version must be representable as a long without loss of precision or null
 * _dyanmic_templates must be a map
 * _if_seq_no must be a long or null
 * _if_primary_term must be a long or null
 *
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 */
class ExternalIngestMetadata extends Metadata {
    protected final ZonedDateTime timestamp;
    protected String index;
    protected static final String INDEX = "_index";

    protected String id;
    protected static final String ID = "_id";

    protected String routing;
    protected static final String ROUTING = "_routing";

    protected String versionType;
    protected static final String VERSION_TYPE = "_version_type";

    protected long version;
    protected static final String VERSION = "_version";

    protected Number ifSeqNo;
    protected static final String IF_SEQ_NO = "_if_seq_no";

    protected Number ifPrimaryTerm;
    protected static final String IF_PRIMARY_TERM = "_if_primary_term";

    protected Map<String, String> dynamicTemplates;
    protected static final String DYNAMIC_TEMPLATES = "_dynamic_templates";

    static final Set<String> VERSION_TYPE_VALUES = Arrays.stream(VersionType.values())
        .map(vt -> VersionType.toString(vt))
        .collect(Collectors.toSet());

    public static Set<String> KEYS = Set.of(INDEX, ID, ROUTING, VERSION_TYPE, VERSION, IF_SEQ_NO, IF_PRIMARY_TERM, DYNAMIC_TEMPLATES);
    protected Set<String> removed = Sets.newHashSetWithExpectedSize(KEYS.size());

    ExternalIngestMetadata(ZonedDateTime timestamp, String index, String id, String routing, String versionType, long version) {
        this.timestamp = timestamp;
        this.index = index;
        this.id = id;
        this.routing = routing;
        this.versionType = versionType;
        this.version = version;
    }

    // These are available to scripts
    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRouting() {
        return routing;
    }

    public void setRouting(String routing) {
        this.routing = routing;
    }

    public String getVersionType() {
        return versionType;
    }

    public void setVersionType(String versionType) {
        if (versionType != null && VERSION_TYPE_VALUES.contains(versionType) == false) {
            throw new IllegalArgumentException(
                "_version_type must be a null or one of ["
                    + VERSION_TYPE_VALUES.stream().sorted().collect(Collectors.joining(", "))
                    + "] but was ["
                    + versionType
                    + "]"
            );
        }
        this.versionType = versionType;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    // These are not available to scripts
    public Number getIfSeqNo() {
        return ifSeqNo;
    }

    public Number getIfPrimaryTerm() {
        return ifPrimaryTerm;
    }

    public Map<String, String> getDynamicTemplates() {
        return dynamicTemplates;
    }

    protected static String toNullableString(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            return str;
        }
        throw new IllegalArgumentException(
            key + " must be null or a String but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    protected static long toLong(String key, Object value) {
        if (value instanceof Number number) {
            long version = number.longValue();
            // did we round?
            if (number.doubleValue() == version) {
                return version;
            }
        }
        throw new IllegalArgumentException(
            key
                + " may only be set to an int or a long but was ["
                + value
                + "]"
                + (value != null ? " with type [" + value.getClass().getName() + "]" : "")
        );
    }

    protected static Number toNullableLong(String key, Object value) {
        if (value == null) {
            return null;
        }
        return toLong(key, value);
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, String> toMapOrNull(String key, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            return (Map<String, String>) value;
        }
        throw new IllegalArgumentException(
            key + " must be a null or a Map but was [" + value + "] with type [" + value.getClass().getName() + "]"
        );
    }

    @Override
    public boolean isMetadata(Object key) {
        if (key instanceof String str) {
            return KEYS.contains(str);
        }
        return false;
    }

    @Override
    public Object put(String key, Object value) {
        Object old;
        switch (key) {
            case INDEX -> {
                old = getIndex();
                setIndex(toNullableString(key, value));
            }
            case ID -> {
                old = getId();
                setId(toNullableString(key, value));
            }
            case ROUTING -> {
                old = getRouting();
                setRouting(toNullableString(key, value));
                return old;
            }
            case VERSION_TYPE -> {
                old = getVersionType();
                setVersionType(toNullableString(key, value));
                return old;
            }
            case VERSION -> {
                old = getVersion();
                setVersion(toLong(key, value));
            }
            case IF_SEQ_NO -> {
                old = getIfSeqNo();
                ifSeqNo = toNullableLong(key, value);
            }
            case IF_PRIMARY_TERM -> {
                old = getIfPrimaryTerm();
                ifPrimaryTerm = toNullableLong(key, value);
            }
            case DYNAMIC_TEMPLATES -> {
                old = getDynamicTemplates();
                dynamicTemplates = toMapOrNull(key, value);
            }
            default -> throw new IllegalArgumentException(unexpectedMessage("put", key));
        }
        removed.remove(key);
        return old;
    }

    public Object get(Object key) {
        if (key instanceof String strKey) {
            if (removed.contains(strKey)) {
                return null;
            }
            switch (strKey) {
                case INDEX:
                    return getIndex();
                case ID:
                    return getId();
                case ROUTING:
                    return getRouting();
                case VERSION_TYPE:
                    return getVersionType();
                case VERSION:
                    return getVersion();
                case IF_SEQ_NO:
                    return getIfSeqNo();
                case IF_PRIMARY_TERM:
                    return getIfPrimaryTerm();
                case DYNAMIC_TEMPLATES:
                    return getDynamicTemplates();
                default:
            }
        }
        throw new IllegalArgumentException(unexpectedMessage("get", key));
    }

    public Object remove(Object key) {
        if (key instanceof String strKey) {
            if (isMetadata(key) == false) {
                throw new IllegalArgumentException(unexpectedMessage("remove", key));
            }
            Object old = get(key);
            switch (strKey) {
                case INDEX -> setIndex(null);
                case ID -> setId(null);
                case ROUTING -> setRouting(null);
                case VERSION_TYPE -> setVersionType(null);
                case VERSION -> throw new IllegalArgumentException("Cannot remove [" + key + "]");
                case IF_SEQ_NO -> ifSeqNo = null;
                case IF_PRIMARY_TERM -> ifPrimaryTerm = null;
                case DYNAMIC_TEMPLATES -> dynamicTemplates = null;
                default -> throw new IllegalArgumentException(unexpectedMessage("remove", key));
            }
            removed.add(strKey);
            return old;
        }
        throw new IllegalArgumentException(unexpectedMessage("remove", key));
    }

    protected String unexpectedMessage(String action, Object key) {
        return "unexpected "
            + action
            + " with key ["
            + key
            + "] expected key to be one of ["
            + KEYS.stream().sorted().collect(Collectors.joining(", "))
            + "]";
    }

    public List<String> keys() {
        Set<String> keys = new HashSet<>(KEYS);
        keys.removeAll(removed);
        return keys.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public boolean exists(Object key) {
        if (key instanceof String str) {
            return KEYS.contains(str) && removed.contains(str) == false;
        }
        return false;
    }

    @Override
    public int size() {
        return KEYS.size() - removed.size();
    }
}
