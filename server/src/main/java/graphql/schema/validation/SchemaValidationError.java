package graphql.schema.validation;

import static graphql.Assert.assertNotNull;

public class SchemaValidationError {

    private final SchemaValidationErrorType errorType;
    private final String description;

    public SchemaValidationError(SchemaValidationErrorType errorType, String description) {
        assertNotNull(errorType, "error type can not be null");
        assertNotNull(description, "error description can not be null");
        this.errorType = errorType;
        this.description = description;
    }

    public SchemaValidationErrorType getErrorType() {
        return errorType;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public int hashCode() {
        return errorType.hashCode() ^ description.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SchemaValidationError)) {
            return false;
        }
        SchemaValidationError that = (SchemaValidationError) other;
        return this.errorType.equals(that.errorType) && this.description.equals(that.description);
    }
}
