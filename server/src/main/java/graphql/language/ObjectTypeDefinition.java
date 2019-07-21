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
public class ObjectTypeDefinition extends AbstractNode<ObjectTypeDefinition> implements TypeDefinition<ObjectTypeDefinition>, DirectivesContainer<ObjectTypeDefinition>, NamedNode<ObjectTypeDefinition> {
    private final String name;
    private final Description description;
    private final List<Type> implementz;
    private final List<Directive> directives;
    private final List<FieldDefinition> fieldDefinitions;

    public static final String CHILD_IMPLEMENTZ = "implementz";
    public static final String CHILD_DIRECTIVES = "directives";
    public static final String CHILD_FIELD_DEFINITIONS = "fieldDefinitions";

    @Internal
    protected ObjectTypeDefinition(String name,
                                   List<Type> implementz,
                                   List<Directive> directives,
                                   List<FieldDefinition> fieldDefinitions,
                                   Description description,
                                   SourceLocation sourceLocation,
                                   List<Comment> comments,
                                   IgnoredChars ignoredChars,
                                   Map<String, String> additionalData) {
        super(sourceLocation, comments, ignoredChars, additionalData);
        this.name = name;
        this.implementz = implementz;
        this.directives = directives;
        this.fieldDefinitions = fieldDefinitions;
        this.description = description;
    }

    /**
     * alternative to using a Builder for convenience
     *
     * @param name of the object type
     */
    public ObjectTypeDefinition(String name) {
        this(name, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), IgnoredChars.EMPTY, emptyMap());
    }

    public List<Type> getImplements() {
        return new ArrayList<>(implementz);
    }

    @Override
    public List<Directive> getDirectives() {
        return new ArrayList<>(directives);
    }

    public List<FieldDefinition> getFieldDefinitions() {
        return new ArrayList<>(fieldDefinitions);
    }

    @Override
    public String getName() {
        return name;
    }

    public Description getDescription() {
        return description;
    }

    @Override
    public List<Node> getChildren() {
        List<Node> result = new ArrayList<>();
        result.addAll(implementz);
        result.addAll(directives);
        result.addAll(fieldDefinitions);
        return result;
    }

    @Override
    public NodeChildrenContainer getNamedChildren() {
        return newNodeChildrenContainer()
                .children(CHILD_IMPLEMENTZ, implementz)
                .children(CHILD_DIRECTIVES, directives)
                .children(CHILD_FIELD_DEFINITIONS, fieldDefinitions)
                .build();
    }

    @Override
    public ObjectTypeDefinition withNewChildren(NodeChildrenContainer newChildren) {
        return transform(builder -> builder.implementz(newChildren.getChildren(CHILD_IMPLEMENTZ))
                .directives(newChildren.getChildren(CHILD_DIRECTIVES))
                .fieldDefinitions(newChildren.getChildren(CHILD_FIELD_DEFINITIONS)));
    }

    @Override
    public boolean isEqualTo(Node o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectTypeDefinition that = (ObjectTypeDefinition) o;
        return NodeUtil.isEqualTo(this.name, that.name);
    }

    @Override
    public ObjectTypeDefinition deepCopy() {
        return new ObjectTypeDefinition(name,
                deepCopy(implementz),
                deepCopy(directives),
                deepCopy(fieldDefinitions),
                description,
                getSourceLocation(),
                getComments(),
                getIgnoredChars(),
                getAdditionalData());
    }

    @Override
    public String toString() {
        return "ObjectTypeDefinition{" +
                "name='" + name + '\'' +
                ", implements=" + implementz +
                ", directives=" + directives +
                ", fieldDefinitions=" + fieldDefinitions +
                '}';
    }

    @Override
    public TraversalControl accept(TraverserContext<Node> context, NodeVisitor visitor) {
        return visitor.visitObjectTypeDefinition(this, context);
    }

    public static Builder newObjectTypeDefinition() {
        return new Builder();
    }

    public ObjectTypeDefinition transform(Consumer<Builder> builderConsumer) {
        Builder builder = new Builder(this);
        builderConsumer.accept(builder);
        return builder.build();
    }

    public static final class Builder implements NodeBuilder {
        private SourceLocation sourceLocation;
        private List<Comment> comments = new ArrayList<>();
        private String name;
        private Description description;
        private List<Type> implementz = new ArrayList<>();
        private List<Directive> directives = new ArrayList<>();
        private List<FieldDefinition> fieldDefinitions = new ArrayList<>();
        private IgnoredChars ignoredChars = IgnoredChars.EMPTY;
        private Map<String, String> additionalData = new LinkedHashMap<>();

        private Builder() {
        }

        private Builder(ObjectTypeDefinition existing) {
            this.sourceLocation = existing.getSourceLocation();
            this.comments = existing.getComments();
            this.name = existing.getName();
            this.description = existing.getDescription();
            this.directives = existing.getDirectives();
            this.implementz = existing.getImplements();
            this.fieldDefinitions = existing.getFieldDefinitions();
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

        public Builder description(Description description) {
            this.description = description;
            return this;
        }

        public Builder implementz(List<Type> implementz) {
            this.implementz = implementz;
            return this;
        }

        public Builder implementz(Type implement) {
            this.implementz.add(implement);
            return this;
        }

        public Builder directives(List<Directive> directives) {
            this.directives = directives;
            return this;
        }

        public Builder directive(Directive directive) {
            this.directives.add(directive);
            return this;
        }

        public Builder fieldDefinitions(List<FieldDefinition> fieldDefinitions) {
            this.fieldDefinitions = fieldDefinitions;
            return this;
        }

        public Builder fieldDefinition(FieldDefinition fieldDefinition) {
            this.fieldDefinitions.add(fieldDefinition);
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

        public ObjectTypeDefinition build() {
            return new ObjectTypeDefinition(name,
                    implementz,
                    directives,
                    fieldDefinitions,
                    description,
                    sourceLocation,
                    comments,
                    ignoredChars,
                    additionalData);
        }
    }
}
