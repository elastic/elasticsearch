package graphql.util;

import graphql.PublicApi;

import java.util.Objects;

/**
 * General position of a Node inside a parent.
 *
 * Can be an index or a name with an index.
 */
@PublicApi
public class NodeLocation {

    private final String name;
    private final int index;

    public NodeLocation(String name, int index) {
        this.name = name;
        this.index = index;
    }

    /**
     * @return the name or null if there is no name
     */
    public String getName() {
        return name;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "NodeLocation{" +
                "name='" + name + '\'' +
                ", index=" + index +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeLocation that = (NodeLocation) o;
        return index == that.index &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, index);
    }
}
