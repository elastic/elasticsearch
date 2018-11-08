package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;

import java.util.Objects;

/**
 * A request to delete a machine learning filter
 */
public class DeleteFilterRequest implements Validatable {

    private final String filter_id;

    public DeleteFilterRequest(String filter_id) {
        this.filter_id = Objects.requireNonNull(filter_id, "filter id is required");
    }

    public String getId() {
        return filter_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter_id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DeleteFilterRequest other = (DeleteFilterRequest) obj;

        return Objects.equals(filter_id, other.filter_id);
    }
}
