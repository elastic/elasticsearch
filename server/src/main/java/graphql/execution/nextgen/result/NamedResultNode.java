package graphql.execution.nextgen.result;

import graphql.Internal;

@Internal
public class NamedResultNode {
    private final String name;
    private final ExecutionResultNode node;

    public NamedResultNode(String name, ExecutionResultNode node) {
        this.name = name;
        this.node = node;
    }

    public String getName() {
        return name;
    }

    public ExecutionResultNode getNode() {
        return node;
    }

    public NamedResultNode withNode(ExecutionResultNode newNode) {
        return new NamedResultNode(name, newNode);
    }
}
