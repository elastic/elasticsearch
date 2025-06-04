/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SuggestProfilesRequest extends LegacyActionRequest {

    private final Set<String> dataKeys;
    /**
     * String to search name related fields of a profile document
     */
    private final String name;
    private final int size;
    @Nullable
    private final Hint hint;

    public SuggestProfilesRequest(Set<String> dataKeys, String name, int size, Hint hint) {
        this.dataKeys = Objects.requireNonNull(dataKeys, "data parameter must not be null");
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.size = size;
        this.hint = hint;
    }

    public SuggestProfilesRequest(StreamInput in) throws IOException {
        super(in);
        this.dataKeys = in.readCollectionAsSet(StreamInput::readString);
        this.name = in.readOptionalString();
        this.size = in.readVInt();
        this.hint = in.readOptionalWriteable(Hint::new);
    }

    public Set<String> getDataKeys() {
        return dataKeys;
    }

    public String getName() {
        return name;
    }

    public int getSize() {
        return size;
    }

    public Hint getHint() {
        return hint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(dataKeys);
        out.writeOptionalString(name);
        out.writeVInt(size);
        out.writeOptionalWriteable(hint);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (size < 0) {
            validationException = addValidationError("[size] parameter cannot be negative but was [" + size + "]", validationException);
        }
        if (hint != null) {
            validationException = hint.validate(validationException);
        }
        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "SuggestProfiles{" + "name='" + name + "', hint=" + hint + '}';
    }

    public static class Hint implements Writeable {
        @Nullable
        private final List<String> uids;
        @Nullable
        private final Map<String, List<String>> labels;

        public Hint(List<String> uids, Map<String, Object> labelsInput) {
            this.uids = uids == null ? null : List.copyOf(uids);
            // Convert the data type. Business logic, e.g. single key, is enforced in request validation
            if (labelsInput != null) {
                final HashMap<String, List<String>> labels = new HashMap<>();
                for (Map.Entry<String, Object> entry : labelsInput.entrySet()) {
                    final Object value = entry.getValue();
                    if (value instanceof String) {
                        labels.put(entry.getKey(), List.of((String) value));
                    } else if (value instanceof final List<?> listValue) {
                        final ArrayList<String> values = new ArrayList<>();
                        for (Object v : listValue) {
                            if (v instanceof final String stringValue) {
                                values.add(stringValue);
                            } else {
                                throw new ElasticsearchParseException("[labels] hint supports either string value or list of strings");
                            }
                        }
                        labels.put(entry.getKey(), List.copyOf(values));
                    } else {
                        throw new ElasticsearchParseException("[labels] hint supports either string or list of strings as its value");
                    }
                }
                this.labels = Map.copyOf(labels);
            } else {
                this.labels = null;
            }
        }

        public Hint(StreamInput in) throws IOException {
            this.uids = in.readStringCollectionAsList();
            this.labels = in.readMapOfLists(StreamInput::readString);
        }

        public List<String> getUids() {
            return uids;
        }

        public Tuple<String, List<String>> getSingleLabel() {
            if (labels == null) {
                return null;
            }
            assert labels.size() == 1 : "labels hint support exactly one key";
            final String labelKey = labels.keySet().iterator().next();
            final List<String> labelValues = labels.get(labelKey);
            assert false == labelValues.isEmpty() : "label values cannot be empty";
            return new Tuple<>(labelKey, labelValues);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(uids);
            out.writeMap(labels, StreamOutput::writeStringCollection);
        }

        private ActionRequestValidationException validate(ActionRequestValidationException validationException) {
            if (uids == null && labels == null) {
                return addValidationError("[hint] parameter cannot be empty", validationException);
            }

            if (uids != null && uids.isEmpty()) {
                validationException = addValidationError("[uids] hint cannot be empty", validationException);
            }

            if (labels != null) {
                if (labels.size() != 1) {
                    return addValidationError(
                        "[labels] hint supports a single key, got [" + Strings.collectionToCommaDelimitedString(labels.keySet()) + "]",
                        validationException
                    );
                }

                final String key = labels.keySet().iterator().next();
                if (key.contains("*")) {
                    validationException = addValidationError("[labels] hint key cannot contain wildcard", validationException);
                }

                final List<String> values = labels.get(key);
                if (values.isEmpty()) {
                    validationException = addValidationError("[labels] hint value cannot be empty", validationException);
                }
            }
            return validationException;
        }

        @Override
        public String toString() {
            return "Hint{" + "uids=" + uids + ", labels=" + labels + '}';
        }
    }
}
