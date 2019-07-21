package graphql.validation;


import graphql.ErrorType;
import graphql.GraphQLError;
import graphql.GraphqlErrorHelper;
import graphql.language.SourceLocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ValidationError implements GraphQLError {

    private final String message;
    private final List<SourceLocation> locations = new ArrayList<>();
    private final String description;
    private final ValidationErrorType validationErrorType;
    private final List<String> queryPath;

    public ValidationError(ValidationErrorType validationErrorType) {
        this(validationErrorType, (SourceLocation) null, null);
    }

    public ValidationError(ValidationErrorType validationErrorType, SourceLocation sourceLocation, String description) {
        this(validationErrorType, nullOrList(sourceLocation), description, null);
    }

    public ValidationError(ValidationErrorType validationErrorType, SourceLocation sourceLocation, String description, List<String> queryPath) {
        this(validationErrorType, nullOrList(sourceLocation), description, queryPath);
    }

    public ValidationError(ValidationErrorType validationErrorType, List<SourceLocation> sourceLocations, String description) {
        this(validationErrorType, sourceLocations, description, null);
    }

    public ValidationError(ValidationErrorType validationErrorType, List<SourceLocation> sourceLocations, String description, List<String> queryPath) {
        this.validationErrorType = validationErrorType;
        if (sourceLocations != null)
            this.locations.addAll(sourceLocations);
        this.description = description;
        this.message = mkMessage(validationErrorType, description, queryPath);
        this.queryPath = queryPath;
    }

    private static List<SourceLocation> nullOrList(SourceLocation sourceLocation) {
        return sourceLocation == null ? null : Collections.singletonList(sourceLocation);
    }

    private String mkMessage(ValidationErrorType validationErrorType, String description, List<String> queryPath) {
        return String.format("Validation error of type %s: %s%s", validationErrorType, description, toPath(queryPath));
    }

    private String toPath(List<String> queryPath) {
        if (queryPath == null) {
            return "";
        }
        return String.format(" @ '%s'", queryPath.stream().collect(Collectors.joining("/")));
    }

    public ValidationErrorType getValidationErrorType() {
        return validationErrorType;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public List<SourceLocation> getLocations() {
        return locations;
    }

    @Override
    public ErrorType getErrorType() {
        return ErrorType.ValidationError;
    }

    public List<String> getQueryPath() {
        return queryPath;
    }


    @Override
    public String toString() {
        return "ValidationError{" +
                "validationErrorType=" + validationErrorType +
                ", queryPath=" + queryPath +
                ", message=" + message +
                ", locations=" + locations +
                ", description='" + description + '\'' +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return GraphqlErrorHelper.equals(this, o);
    }

    @Override
    public int hashCode() {
        return GraphqlErrorHelper.hashCode(this);
    }

}
