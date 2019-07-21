package graphql.execution;

import graphql.PublicApi;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLUnionType;

import java.util.Map;

@PublicApi
public class TypeResolutionParameters {

    private final GraphQLInterfaceType graphQLInterfaceType;
    private final GraphQLUnionType graphQLUnionType;
    private final MergedField field;
    private final Object value;
    private final Map<String, Object> argumentValues;
    private final GraphQLSchema schema;
    private final Object context;

    private TypeResolutionParameters(GraphQLInterfaceType graphQLInterfaceType, GraphQLUnionType graphQLUnionType,
                                     MergedField field, Object value, Map<String, Object> argumentValues, GraphQLSchema schema, final Object context) {
        this.graphQLInterfaceType = graphQLInterfaceType;
        this.graphQLUnionType = graphQLUnionType;
        this.field = field;
        this.value = value;
        this.argumentValues = argumentValues;
        this.schema = schema;
        this.context = context;
    }

    public GraphQLInterfaceType getGraphQLInterfaceType() {
        return graphQLInterfaceType;
    }

    public GraphQLUnionType getGraphQLUnionType() {
        return graphQLUnionType;
    }

    public MergedField getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

    public Map<String, Object> getArgumentValues() {
        return argumentValues;
    }

    public GraphQLSchema getSchema() {
        return schema;
    }

    public static Builder newParameters() {
        return new Builder();
    }

    public Object getContext() {
        return context;
    }

    public static class Builder {

        private MergedField field;
        private GraphQLInterfaceType graphQLInterfaceType;
        private GraphQLUnionType graphQLUnionType;
        private Object value;
        private Map<String, Object> argumentValues;
        private GraphQLSchema schema;
        private Object context;

        public Builder field(MergedField field) {
            this.field = field;
            return this;
        }

        public Builder graphQLInterfaceType(GraphQLInterfaceType graphQLInterfaceType) {
            this.graphQLInterfaceType = graphQLInterfaceType;
            return this;
        }

        public Builder graphQLUnionType(GraphQLUnionType graphQLUnionType) {
            this.graphQLUnionType = graphQLUnionType;
            return this;
        }

        public Builder value(Object value) {
            this.value = value;
            return this;
        }

        public Builder argumentValues(Map<String, Object> argumentValues) {
            this.argumentValues = argumentValues;
            return this;
        }

        public Builder schema(GraphQLSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder context(Object context) {
            this.context = context;
            return this;
        }

        public TypeResolutionParameters build() {
            return new TypeResolutionParameters(graphQLInterfaceType, graphQLUnionType, field, value, argumentValues, schema, context);
        }
    }
}
