package graphql.execution;

import static java.lang.String.format;

import java.util.Collections;
import java.util.List;

import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphQLException;
import graphql.PublicApi;
import graphql.language.SourceLocation;
import graphql.language.VariableDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;

/**
 * This is thrown if a non nullable value is coerced to a null value
 */
@PublicApi
public class NonNullableValueCoercedAsNullException extends GraphQLException implements GraphQLError {
    private List<SourceLocation> sourceLocations;

    public NonNullableValueCoercedAsNullException(VariableDefinition variableDefinition, GraphQLType graphQLType) {
        super(format("Variable '%s' has coerced Null value for NonNull type '%s'",
                variableDefinition.getName(), GraphQLTypeUtil.simplePrint(graphQLType)));
        this.sourceLocations = Collections.singletonList(variableDefinition.getSourceLocation());
    }

    public NonNullableValueCoercedAsNullException(VariableDefinition variableDefinition, String fieldName, GraphQLType graphQLType) {
        super(format("Field '%s' of variable '%s' has coerced Null value for NonNull type '%s'",
                fieldName, variableDefinition.getName(), GraphQLTypeUtil.simplePrint(graphQLType)));
        this.sourceLocations = Collections.singletonList(variableDefinition.getSourceLocation());
    }

    public NonNullableValueCoercedAsNullException(GraphQLInputObjectField inputTypeField) {
        super(format("Input field '%s' has coerced Null value for NonNull type '%s'",
                inputTypeField.getName(), GraphQLTypeUtil.simplePrint(inputTypeField.getType())));
    }

    @Override
    public List<SourceLocation> getLocations() {
        return sourceLocations;
    }

    @Override
    public ErrorType getErrorType() {
        return ErrorType.ValidationError;
    }
}
