package graphql.execution;

import graphql.Assert;

import java.util.UUID;

/**
 * This opaque identifier is used to identify a unique query execution
 */
public class ExecutionId {

    /**
     * Create an unique identifier from the given string
     *
     * @return a query execution identifier
     */
    public static ExecutionId generate() {
        return new ExecutionId(UUID.randomUUID().toString());
    }

    /**
     * Create an identifier from the given string
     *
     * @param id the string to wrap
     *
     * @return a query identifier
     */
    public static ExecutionId from(String id) {
        return new ExecutionId(id);
    }

    private final String id;

    private ExecutionId(String id) {
        Assert.assertNotNull(id, "You must provided a non null id");
        this.id = id;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutionId that = (ExecutionId) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
