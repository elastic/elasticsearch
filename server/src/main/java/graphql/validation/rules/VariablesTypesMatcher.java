package graphql.validation.rules;


import graphql.Internal;
import graphql.language.Value;
import graphql.schema.GraphQLType;

import static graphql.schema.GraphQLNonNull.nonNull;
import static graphql.schema.GraphQLTypeUtil.isList;
import static graphql.schema.GraphQLTypeUtil.isNonNull;
import static graphql.schema.GraphQLTypeUtil.unwrapOne;

@Internal
public class VariablesTypesMatcher {

    public boolean doesVariableTypesMatch(GraphQLType variableType, Value variableDefaultValue, GraphQLType expectedType) {
        return checkType(effectiveType(variableType, variableDefaultValue), expectedType);
    }

    public GraphQLType effectiveType(GraphQLType variableType, Value defaultValue) {
        if (defaultValue == null) {
            return variableType;
        }
        if (isNonNull(variableType)) {
            return variableType;
        }
        return nonNull(variableType);
    }

    @SuppressWarnings("SimplifiableIfStatement")
    private boolean checkType(GraphQLType actualType, GraphQLType expectedType) {

        if (isNonNull(expectedType)) {
            if (isNonNull(actualType)) {
                return checkType(unwrapOne(actualType), unwrapOne(expectedType));
            }
            return false;
        }

        if (isNonNull(actualType)) {
            return checkType(unwrapOne(actualType), expectedType);
        }


        if (isList(actualType) && isList(expectedType)) {
            return checkType(unwrapOne(actualType), unwrapOne(expectedType));
        }
        return actualType == expectedType;
    }

}
