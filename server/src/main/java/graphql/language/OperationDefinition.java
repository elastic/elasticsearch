package graphql.language;


import graphql.Internal;
import graphql.PublicApi;
import graphql.util.TraversalControl;
import graphql.util.TraverserContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.Assert.assertNotNull;
import static graphql.language.NodeChildrenContainer.newNodeChildrenContainer;
import static java.util.Collections.emptyMap;

@PublicApi
public class OperationDefinition extends AbstractNode<OperationDefinition> implements Definition<OperationDefinition>, SelectionSetContainer<OperationDefinition>, DirectivesContainer<OperationDefinition> {

    public enum Operation {
        QUERY, MUTATION, SUBSCRIPTION
    }

    private final String name;

    private final Operation operation;
    private final List<VariableDefinition> variableDefinitions;
    private final List<Directive> directives;
    private final SelectionSet selectionSet;

    public static final String CHILD_VARIABLE_DEFINITIONS = "variableDefinitions";
    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_SELECTION_SET = "selectionSet";

    @Internal
    protected OperationDefinition(String name,
                                  Operation operation,
                                  List<VariableDefinition> variableDefinitions,
                                  List<Directive> directives,
                                  SelectionSet selectionSet,
                                  SourceLocation sourceLocation,
                                  List<Comment> comments,
                                  IgnoredChars ignoredChars,
                                  Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.operation = operation;
        this.variableDefinitions = variableDefinitions;
        this.directives = directives;
        this.selectionSet = selectionSet;
    }

    public OperationDefinition(String name,
                               Operation operation) {
        this(name, operation, new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public OperationDefinition(String name) {
        this(name, null, new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(variableDefinitions);
        result.addAll(directives);
        result.add(selectionSet);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_VARIABLE_DEFINITIONS, variableDefinitions)
                .children(CHILD_DIRECTIVES, directives)
                .child(CHILD_SELECTION_SET, selectionSet)
                .build();
    }

    @Override
    public OperationDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder
                .variableDefinitions(newChildren.getChildren(CHILD_VARIABLE_DEFINITIONS))
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))
                .selectionSet(newChildren.getChildOrNull(CHILD_SELECTION_SET))
        );
    }

    public String getName() {
        return name;
    }

    public Operation getOperation() {
        return operation;
    }

    public List<VariableDefinition> getVariableDefinitions() {
        return new ArrayList<>(variableDefinitions);
    }

    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    @Override
    public SelectionSet getSelectionSet() {
        return selectionSet;
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OperationDefinition that = (OperationDefinition) o;

        return NodeUtil.isEqualTo(this.name, that.name) && operation == that.operation;

    }

    @Override
    public OperationDefinition deepCopy() {
        return new OperationDefinition(name,
                operation,
                deepCopy(variableDefinitions),
                deepCopy(directives),
                deepCopy(selectionSet),
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "OperationDefinition{" +
                "name='" + name + '\'' +
                ", operation=" + operation +
                ", variableDefinitions=" + variableDefinitions +
                ", directives=" + directives +
                ", selectionSet=" + selectionSet +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitOperationDefinition(this, context);
    }

    public static Builder newOperationDefinition() {
        return new Builder();
    }

    public OperationDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Operation operation;
        private List<VariableDefinition> variableDefinitions = new ArrayList<>();
        private List<Directive> directives = new ArrayList<>();
        private SelectionSet selectionSet;
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(OperationDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.operation = existing.getOperation();
            this.variableDefinitions = existing.getVariableDefinitions();
            this.directives = existing.getDirectives();
            this.selectionSet = existing.getSelectionSet();
            this.ignoredChars = existing.getIgnoredChars();
            this.additionalData = existing.getAdditionalData();
        }


        public Builder sourceLocation(SourceLocation sourceLocation) {
            this.sourceLocation = sourceLocation;
            return this;
        }

        public Builder comments(List<Comment> comments) {
            this.comments = comments;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder operation(Operation operation) {
            this.operation = operation;
            return this;
        }

        public Builder variableDefinitions(List<VariableDefinition> variableDefinitions) {
            this.variableDefinitions = variableDefinitions;
            return this;
        }

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder selectionSet(SelectionSet selectionSet) {
            this.selectionSet = selectionSet;
            return this;
        }

        public Builder ignoredChars(IgnoredChars ignoredChars) {
            this.ignoredChars = ignoredChars;
            return this;
        }

        public Builder additionalData(Map<String, String> additionalData) {
            this.additionalData = assertNotNull(additionalData);
            return this;
        }

        public Builder additionalData(String key, String value) {
            this.additionalData.put(key, value);
            return this;
        }

        public OperationDefinition build() {
            return new OperationDefinition(
                    name,
                    operation,
                    variableDefinitions,
                    directives,
                    selectionSet,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
