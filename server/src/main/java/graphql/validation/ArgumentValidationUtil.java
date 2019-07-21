package graphql.validation;

import graphql.language.Argument;
import graphql.language.ObjectField;
import graphql.language.Value;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ArgumentValidationUtil extends ValidationUtil {

    private final List<String> argumentNames = new ArrayList<>();
    private Value argumentValue;
    private String errorMessage;
    private final List<Object> arguments = new ArrayList<>();

    private final String argumentName;

    public ArgumentValidationUtil(Argument argument) {
        argumentName = argument.getName();
        argumentValue = argument.getValue();
    }

    @Override
    protected void handleNullError(Value value, GraphQLType type) {
        errorMessage = "must not be null";
        argumentValue = value;
    }

    @Override
    protected void handleScalarError(Value value, GraphQLScalarType type) {
        errorMessage = "is not a valid '%s'";
        arguments.add(type.getName());
        argumentValue = value;
    }

    @Override
    protected void handleEnumError(Value value, GraphQLEnumType type) {
        errorMessage = "is not a valid '%s'";
        arguments.add(type.getName());
        argumentValue = value;
    }

    @Override
    protected void handleNotObjectError(Value value, GraphQLInputObjectType type) {
        errorMessage = "must be an object type";
    }

    @Override
    protected void handleMissingFieldsError(Value value, GraphQLInputObjectType type, Set<String> missingFields) {
        errorMessage = "is missing required fields '%s'";
        arguments.add(missingFields);
    }

    @Override
    protected void handleExtraFieldError(Value value, GraphQLInputObjectType type, ObjectField objectField) {
        errorMessage = "contains a field not in '%s': '%s'";
        arguments.add(type.getName());
        arguments.add(objectField.getName());
    }

    @Override
    protected void handleFieldNotValidError(ObjectField objectField, GraphQLInputObjectType type) {
        argumentNames.add(0, objectField.getName());
    }

    @Override
    protected void handleFieldNotValidError(Value value, GraphQLType type, int index) {
        argumentNames.add(0, String.format("[%s]", index));
    }

    public String getMessage() {
        StringBuilder argument = new StringBuilder(argumentName);
        for (String name : argumentNames) {
            if (name.startsWith("[")) {
                argument.append(name);
            } else {
                argument.append(".").append(name);
            }
        }
        arguments.add(0, argument.toString());
        arguments.add(1, argumentValue);

        String message = "argument '%s' with value '%s'" + " " + errorMessage;

        return String.format(message, arguments.toArray());
    }
}
