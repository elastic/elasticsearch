package graphql.schema.diff;

import graphql.PublicApi;
import graphql.language.TypeKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * This represents the events that the {@link SchemaDiff} outputs.
 */
@PublicApi
public class DiffEvent {

    private final DiffLevel level;
    private final DiffCategory category;
    private final TypeKind typeOfType;
    private final String typeName;
    private final String fieldName;
    private final String reasonMsg;
    private final List<String> components;

    DiffEvent(DiffLevel level, DiffCategory category, String typeName, String fieldName, TypeKind typeOfType, String reasonMsg, List<String> components) {
        this.level = level;
        this.category = category;
        this.typeName = typeName;
        this.fieldName = fieldName;
        this.typeOfType = typeOfType;
        this.reasonMsg = reasonMsg;
        this.components = components;
    }

    public String getTypeName() {
        return typeName;
    }

    public TypeKind getTypeKind() {
        return typeOfType;
    }

    public String getReasonMsg() {
        return reasonMsg;
    }

    public DiffLevel getLevel() {
        return level;
    }

    public String getFieldName() {
        return fieldName;
    }

    public DiffCategory getCategory() {
        return category;
    }

    public List<String> getComponents() {
        return new ArrayList<>(components);
    }

    @Override
    public String toString() {
        return "DifferenceEvent{" +
                " reasonMsg='" + reasonMsg + '\'' +
                ", level=" + level +
                ", category=" + category +
                ", typeName='" + typeName + '\'' +
                ", typeKind=" + typeOfType +
                ", fieldName=" + fieldName +
                '}';
    }

    /**
     * @return  a Builder of Info level diff events
     * @deprecated use {@link DiffEvent#apiInfo()} instead
     */
    @Deprecated
    public static Builder newInfo() {
        return new Builder().level(DiffLevel.INFO);
    }

    public static Builder apiInfo() {
        return new Builder().level(DiffLevel.INFO);
    }

    public static Builder apiDanger() {
        return new Builder().level(DiffLevel.DANGEROUS);
    }

    public static Builder apiBreakage() {
        return new Builder().level(DiffLevel.BREAKING);
    }


    public static class Builder {

        DiffCategory category;
        DiffLevel level;
        String typeName;
        TypeKind typeOfType;
        String reasonMsg;
        String fieldName;
        final List<String> components = new ArrayList<>();

        public Builder level(DiffLevel level) {
            this.level = level;
            return this;
        }


        public Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder typeKind(TypeKind typeOfType) {
            this.typeOfType = typeOfType;
            return this;
        }

        public Builder category(DiffCategory category) {
            this.category = category;
            return this;
        }

        public Builder reasonMsg(String format, Object... args) {
            this.reasonMsg = String.format(format, args);
            return this;
        }

        public Builder components(Object... args) {
            components.addAll(Arrays.stream(args).map(String::valueOf).collect(toList()));
            return this;
        }

        public DiffEvent build() {
            return new DiffEvent(level, category, typeName, fieldName, typeOfType, reasonMsg, components);
        }
    }
}
