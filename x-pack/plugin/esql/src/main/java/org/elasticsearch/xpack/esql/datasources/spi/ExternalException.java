/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.core.QlException;

/**
 * Base type for failures raised while reading from an external data source — an object store, a
 * file in some columnar/row format, a remote HTTP endpoint, etc. Grouping every external-source
 * failure under one type lets callers catch them as a family ({@code catch (ExternalException)})
 * and lets external-read operators surface them with the correct HTTP status.
 * <p>
 * The distinction between server- and client-class failures is carried by the concrete subtype, so
 * the right status falls out of the exception itself rather than from a downstream {@code instanceof}
 * ladder:
 * <ul>
 *     <li>{@link ExternalClientException} &rarr; 400, the request pointed us at something we cannot
 *     read or decode (bad/unsupported input, missing object).</li>
 *     <li>{@link ExternalCredentialsExpiredException} &rarr; 400, session or temporary credentials
 *     used to read the store have expired or been rejected; refresh and re-run.</li>
 *     <li>{@link ExternalServerException} &rarr; 500, a bug or broken invariant in our own reading
 *     code.</li>
 *     <li>{@link ExternalUnavailableException} &rarr; 503, a retryable back-pressure /
 *     temporarily-unavailable condition (a remote-store transport failure, or a node-local admission
 *     condition such as permit exhaustion).</li>
 *     <li>{@link ExternalObjectChangedException} &rarr; 503, the object was rewritten mid-query so a
 *     resume would splice generations. Query-level retryable, but not retried as a storage resume
 *     (the generation pin will keep failing until the query restarts).</li>
 * </ul>
 * Subtypes extend {@link QlException} (rather than {@code QlClientException}/{@code QlServerException})
 * so they can share this single umbrella while each pinning its own status.
 * <p>
 * <b>Structured constructors.</b> Subtypes expose structured constructors
 * ({@link Condition}, {@link StoragePath}, {@code detailCode}, {@code remedy}) that encode the
 * error without embedding the full storage URI — only the object name (last path segment) from
 * {@link StoragePath#objectName()} appears in the message. Reader modules that have additional
 * context (ORC/Parquet library message) may call {@link #setDetail(String)} before throwing to
 * attach a single free-text slot; that detail is appended to {@link #getMessage()} as a colon-separated
 * suffix. The detail must be set on the same node before the exception crosses any boundary.
 * <p>
 * <b>Transport behaviour.</b> Like every other ES|QL exception, these are not registered in
 * {@code ElasticsearchException}'s serialization registry, so crossing a node boundary turns them
 * into a {@code NotSerializableExceptionWrapper}. That wrapper preserves both the public exception
 * name (for example, {@code external_client_exception}) and {@link #status()} (it captures
 * {@code ExceptionsHelper.status(this)} on the sending node and replays it on the receiver), so the
 * name and 400/500/503 distinction survive the data-node &rarr; coordinator hop and the REST layer
 * still maps them correctly. What does <em>not</em> survive is the concrete Java type: a remote
 * receiver cannot {@code instanceof}-check these. That is fine because classification happens
 * co-located with the throw — {@code ExternalFailures.classify} runs inside
 * {@code AsyncExternalSourceOperator} for eager reads and {@code ExternalFieldExtractOperator} for
 * deferred reads, on the node that reads the external source and before any serialization — so no
 * remote consumer ever needs the concrete type.
 */
public abstract class ExternalException extends QlException {

    /**
     * Typed error condition for an external-source failure. The condition encodes the nature of
     * the error without free-text that might embed storage URIs; structured constructors on each
     * subtype use it to build a safe, human-readable message from the object name and a short
     * {@code detailCode} (e.g. "HTTP 403") supplied by the provider.
     */
    public enum Condition {
        /** A transient, non-throttling store failure — connection error, 500/502/504. */
        STORE_UNAVAILABLE {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                StringBuilder sb = new StringBuilder("External store unavailable");
                if (objectName.isEmpty() == false) {
                    sb.append(" reading [").append(objectName).append("]");
                }
                if (detailCode.isEmpty() == false) {
                    sb.append(" (").append(detailCode).append(")");
                }
                if (remedy.isEmpty() == false) {
                    sb.append(". ").append(remedy);
                }
                return sb.toString();
            }
        },
        /** Back-pressure / throttling from the store — 429 / 503. */
        STORE_THROTTLED {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                StringBuilder sb = new StringBuilder("External store throttled");
                if (objectName.isEmpty() == false) {
                    sb.append(" reading [").append(objectName).append("]");
                }
                if (detailCode.isEmpty() == false) {
                    sb.append(" (").append(detailCode).append(")");
                }
                if (remedy.isEmpty() == false) {
                    sb.append(". ").append(remedy);
                }
                return sb.toString();
            }
        },
        /** The object was replaced with a different generation mid-query. */
        OBJECT_CHANGED {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                if (objectName.isEmpty()) {
                    return "External data object was modified during read";
                }
                return "External data object [" + objectName + "] was modified during read";
            }
        },
        /** The credentials lacked permission to read the object or list the prefix. */
        ACCESS_DENIED {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                StringBuilder sb = new StringBuilder("Access denied");
                if (objectName.isEmpty() == false) {
                    sb.append(" reading [").append(objectName).append("]");
                } else {
                    sb.append(" reading external data");
                }
                if (detailCode.isEmpty() == false) {
                    sb.append(" (").append(detailCode).append(")");
                }
                if (remedy.isEmpty() == false) {
                    sb.append(". ").append(remedy);
                }
                return sb.toString();
            }
        },
        /** The requested object does not exist at the storage path. */
        OBJECT_NOT_FOUND {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                if (objectName.isEmpty()) {
                    return "External data object not found";
                }
                return "External data object not found: [" + objectName + "]";
            }
        },
        /** Session or temporary credentials have expired; the caller must refresh and retry. */
        CREDENTIALS_EXPIRED {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                String base = "Session credentials expired or invalid. "
                    + (remedy.isEmpty() ? "Refresh the data source credentials and re-run the query." : remedy);
                return detailCode.isEmpty() ? base : base + " (" + detailCode + ")";
            }
        },
        /** The object's data is corrupt, truncated, or uses an unsupported format variant. */
        MALFORMED_DATA {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                StringBuilder sb = new StringBuilder();
                if (objectName.isEmpty()) {
                    sb.append("Malformed external data");
                } else {
                    sb.append("Malformed data in [").append(objectName).append("]");
                }
                if (detailCode.isEmpty() == false) {
                    sb.append(" (").append(detailCode).append(")");
                }
                return sb.toString();
            }
        },
        /** Object metadata (size, last-modified, ETag) could not be retrieved. */
        METADATA_UNAVAILABLE {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                if (objectName.isEmpty()) {
                    return "Failed to get external data metadata";
                }
                return "Failed to get metadata for [" + objectName + "]";
            }
        },
        /** A listing call failed — the prefix could not be enumerated. */
        LISTING_FAILED {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                StringBuilder sb = new StringBuilder("Failed to list external data objects");
                if (detailCode.isEmpty() == false) {
                    sb.append(" (").append(detailCode).append(")");
                }
                if (remedy.isEmpty() == false) {
                    sb.append(". ").append(remedy);
                }
                return sb.toString();
            }
        },
        /** The request was rejected because the host clock is too far from the store's clock. */
        CLOCK_SKEW {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                return "S3 request rejected due to clock skew: "
                    + "the server clock differs too much from S3. Check that the host clock is NTP-synchronized.";
            }
        },
        /** An unexpected internal failure in our own reading code — likely a bug. */
        CLIENT_BUG {
            @Override
            public String render(String objectName, String detailCode, String remedy) {
                StringBuilder sb = new StringBuilder("Unexpected internal failure reading external source");
                if (detailCode.isEmpty() == false) {
                    sb.append(": ").append(detailCode);
                }
                return sb.toString();
            }
        };

        /**
         * Builds the user-facing message for this condition from the safe object name (last path
         * segment, never the full URI) and an optional {@code detailCode} (e.g. "HTTP 403") and
         * {@code remedy} (actionable advice). Any of the three may be empty.
         */
        public abstract String render(String objectName, String detailCode, String remedy);
    }

    /** The structured condition, or {@code null} when constructed via a legacy free-text constructor. */
    private final Condition condition;
    /** Last path segment only — never the full URI. Empty string when constructed via a legacy constructor. */
    private final String objectName;
    /** Short qualifier such as "HTTP 403 AccessDenied". Empty when not set. */
    private final String detailCode;
    /** Actionable remedy text. Empty when not set. */
    private final String remedy;
    /**
     * Reader-owned free-text annotation, set by the format reader (ORC, Parquet) via
     * {@link #setDetail(String)} before the exception is thrown. Appended to {@link #getMessage()}
     * as a colon-separated suffix. Must be set before the exception crosses any node boundary.
     */
    private volatile String detail;
    /**
     * Dataset context injected by the operator after {@link org.elasticsearch.xpack.esql.datasources.ExternalFailures#classify},
     * e.g. {@code "in dataset [tmax] from data source [noaa] (s3)"}. Appended to {@link #getMessage()}
     * after any {@link #detail}. Must be set before the exception crosses any node boundary.
     */
    private volatile String datasetContext;

    // ---- Legacy constructors (used at the classify() boundary where no StoragePath is available) ----

    protected ExternalException(String message, Throwable cause) {
        super(message, cause);
        this.condition = null;
        this.objectName = "";
        this.detailCode = "";
        this.remedy = "";
    }

    protected ExternalException(Throwable cause, String message, Object... args) {
        super(cause, message, args);
        this.condition = null;
        this.objectName = "";
        this.detailCode = "";
        this.remedy = "";
    }

    protected ExternalException(String message, Object... args) {
        super(message, args);
        this.condition = null;
        this.objectName = "";
        this.detailCode = "";
        this.remedy = "";
    }

    // ---- Structured constructors (used at provider/reader throw sites where StoragePath is available) ----

    /**
     * Structured constructor: builds the message from {@code condition.render(path.objectName(), detailCode, remedy)}.
     * Only the object name (last path segment) is embedded — the bucket, prefix, and full URI
     * are never included. The {@code cause} parameter chains the low-level SDK or I/O exception.
     */
    protected ExternalException(Condition condition, StoragePath path, String detailCode, String remedy, Throwable cause) {
        super(condition.render(path.objectName(), detailCode, remedy), cause);
        this.condition = condition;
        this.objectName = path.objectName();
        this.detailCode = detailCode != null ? detailCode : "";
        this.remedy = remedy != null ? remedy : "";
    }

    /**
     * Structured constructor without a cause. See {@link #ExternalException(Condition, StoragePath, String, String, Throwable)}.
     */
    protected ExternalException(Condition condition, StoragePath path, String detailCode, String remedy) {
        super(condition.render(path.objectName(), detailCode, remedy));
        this.condition = condition;
        this.objectName = path.objectName();
        this.detailCode = detailCode != null ? detailCode : "";
        this.remedy = remedy != null ? remedy : "";
    }

    /**
     * The typed condition that describes this failure, or {@code null} when constructed via a
     * legacy free-text constructor (e.g. at the classify() boundary).
     */
    public Condition condition() {
        return condition;
    }

    /**
     * The object name (last path segment) involved in the failure, or an empty string when not set.
     * Never the full storage URI.
     */
    public String objectName() {
        return objectName;
    }

    /**
     * Sets a short free-text detail for the reader to annotate after construction, before throwing.
     * Appended to {@link #getMessage()} as {@code ": <detail>"}. Must be called on the same node
     * as the throw, before the exception crosses any boundary.
     */
    public void setDetail(String detail) {
        this.detail = detail;
    }

    /**
     * Annotates the exception with the dataset, data source name, and data source type, producing a
     * suffix such as {@code "in dataset [tmax] from data source [noaa] (s3)"}. Omits any component
     * that is {@code null} or empty. Appended to {@link #getMessage()} after any {@link #setDetail detail}.
     * Must be called on the same node as the throw, before the exception crosses any boundary.
     */
    public void setDatasetContext(String datasetName, String datasourceName, String datasourceType) {
        StringBuilder sb = new StringBuilder();
        if (datasetName != null && datasetName.isEmpty() == false) {
            sb.append("in dataset [").append(datasetName).append("]");
        }
        if (datasourceName != null && datasourceName.isEmpty() == false) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append("from data source [").append(datasourceName).append("]");
            if (datasourceType != null && datasourceType.isEmpty() == false) {
                sb.append(" (").append(datasourceType).append(")");
            }
        }
        this.datasetContext = sb.length() > 0 ? sb.toString() : null;
    }

    /**
     * Sets the dataset context suffix directly from a pre-formatted label, e.g.
     * {@code "in dataset [tmax] from data source [noaa] (s3)"}. The label is appended to
     * {@link #getMessage()} after any {@link #setDetail detail}. Use this form when the factory has
     * already assembled the full label string; use {@link #setDatasetContext(String, String, String)}
     * when the components are available individually.
     */
    public void setDatasetLabel(String label) {
        this.datasetContext = label;
    }

    @Override
    public String getMessage() {
        String d = detail;
        String dc = datasetContext;
        String base = (d == null || d.isEmpty()) ? super.getMessage() : super.getMessage() + ": " + d;
        return (dc == null || dc.isEmpty()) ? base : base + " " + dc;
    }
}
