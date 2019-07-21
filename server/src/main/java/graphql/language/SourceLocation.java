package graphql.language;


import graphql.PublicApi;

import java.io.Serializable;

@PublicApi
public class SourceLocation implements Serializable {

    private final int line;
    private final int column;
    private final String sourceName;

    public SourceLocation(int line, int column) {
        this(line, column, null);
    }

    public SourceLocation(int line, int column, String sourceName) {
        this.line = line;
        this.column = column;
        this.sourceName = sourceName;
    }

    public int getLine() {
        return line;
    }

    public int getColumn() {
        return column;
    }

    public String getSourceName() {
        return sourceName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SourceLocation that = (SourceLocation) o;

        if (line != that.line) return false;
        if (column != that.column) return false;
        return sourceName != null ? sourceName.equals(that.sourceName) : that.sourceName == null;
    }

    @Override
    public int hashCode() {
        int result = line;
        result = 31 * result + column;
        result = 31 * result + (sourceName != null ? sourceName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SourceLocation{" +
                "line=" + line +
                ", column=" + column +
                (sourceName != null ? ", sourceName=" + sourceName : "") +
                '}';
    }
}
