package graphql.relay;

import graphql.Assert;
import graphql.PublicApi;

@PublicApi
public class DefaultConnectionCursor implements ConnectionCursor {

    private final String value;

    public DefaultConnectionCursor(String value) {
        Assert.assertTrue(value != null && !value.isEmpty(), "connection value cannot be null or empty");
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultConnectionCursor that = (DefaultConnectionCursor) o;
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return value;
    }
}
