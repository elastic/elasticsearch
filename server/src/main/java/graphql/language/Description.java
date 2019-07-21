package graphql.language;

import graphql.PublicApi;

import java.io.Serializable;

@PublicApi
public class Description implements Serializable {
    public final String content;
    public final SourceLocation sourceLocation;
    public final boolean multiLine;

    public Description(String content, SourceLocation sourceLocation, boolean multiLine) {
        this.content = content;
        this.sourceLocation = sourceLocation;
        this.multiLine = multiLine;
    }

    public String getContent() {
        return content;
    }

    public SourceLocation getSourceLocation() {
        return sourceLocation;
    }

    public boolean isMultiLine() {
        return multiLine;
    }
}
