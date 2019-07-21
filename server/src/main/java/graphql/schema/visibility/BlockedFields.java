package graphql.schema.visibility;

import graphql.Internal;
import graphql.PublicApi;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLFieldsContainer;
import graphql.schema.GraphQLInputFieldsContainer;
import graphql.schema.GraphQLInputObjectField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This helper class will take a list of regular expressions and match them against the fully qualified name
 * of a type and its fields.  So for example an object type called "User" with an inner field called "firstName"
 * will have a fully qualified name of "User.firstName" in terms of pattern matching.
 *
 * Remember that graphql type and fields names MUST be inside the name space "[_A-Za-z][_0-9A-Za-z]*"
 */
@PublicApi
public class BlockedFields implements GraphqlFieldVisibility {

    private final List<Pattern> patterns;

    /**
     * @param patterns the blocked field patterns
     *
     * @deprecated use the {@link #newBlock()} builder pattern instead, as this constructor will be made private in a future version.
     */
    @Internal
    @Deprecated
    public BlockedFields(List<Pattern> patterns) {
        this.patterns = patterns;
    }

    @Override
    public List<GraphQLFieldDefinition> getFieldDefinitions(GraphQLFieldsContainer fieldsContainer) {
        return fieldsContainer.getFieldDefinitions().stream()
                .filter(fieldDefinition -> !block(mkFQN(fieldsContainer.getName(), fieldDefinition.getName())))
                .collect(Collectors.toList());
    }

    @Override
    public GraphQLFieldDefinition getFieldDefinition(GraphQLFieldsContainer fieldsContainer, String fieldName) {
        GraphQLFieldDefinition fieldDefinition = fieldsContainer.getFieldDefinition(fieldName);
        if (fieldDefinition != null) {
            if (block(mkFQN(fieldsContainer.getName(), fieldDefinition.getName()))) {
                fieldDefinition = null;
            }
        }
        return fieldDefinition;
    }

    @Override
    public List<GraphQLInputObjectField> getFieldDefinitions(GraphQLInputFieldsContainer fieldsContainer) {
        return fieldsContainer.getFieldDefinitions().stream()
                .filter(fieldDefinition -> !block(mkFQN(fieldsContainer.getName(), fieldDefinition.getName())))
                .collect(Collectors.toList());
    }

    @Override
    public GraphQLInputObjectField getFieldDefinition(GraphQLInputFieldsContainer fieldsContainer, String fieldName) {
        GraphQLInputObjectField fieldDefinition = fieldsContainer.getFieldDefinition(fieldName);
        if (fieldDefinition != null) {
            if (block(mkFQN(fieldsContainer.getName(), fieldDefinition.getName()))) {
                fieldDefinition = null;
            }
        }
        return fieldDefinition;
    }

    private boolean block(String fqn) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(fqn).matches()) {
                return true;
            }
        }
        return false;
    }

    private String mkFQN(String containerName, String fieldName) {
        return containerName + "." + fieldName;
    }

    public static Builder newBlock() {
        return new Builder();
    }

    public static class Builder {
        private final List<Pattern> patterns = new ArrayList<>();

        public Builder addPattern(String regexPattern) {
            return addCompiledPattern(Pattern.compile(regexPattern));
        }

        public Builder addPatterns(Collection<String> regexPatterns) {
            regexPatterns.forEach(this::addPattern);
            return this;
        }

        public Builder addCompiledPattern(Pattern regex) {
            patterns.add(regex);
            return this;
        }

        public Builder addCompiledPatterns(Collection<Pattern> regexes) {
            regexes.forEach(this::addCompiledPattern);
            return this;
        }

        public BlockedFields build() {
            return new BlockedFields(patterns);
        }
    }
}
