package org.elasticsearch.xpack.core.template;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class IndexTemplateConfig {

    private final String templateName;
    private final String fileName;
    private final String version;
    private final String versionProperty;

    public IndexTemplateConfig(String templateName, String fileName, String version, String versionProperty) {
        this.templateName = templateName;
        this.fileName = fileName;
        this.version = version;
        this.versionProperty = versionProperty;
    }

    public String getFileName() {
        return fileName;
    }

    public String getTemplateName() {
        return templateName;
    }

    public byte[] load() {
        String template = TemplateUtils.loadTemplate("/" + fileName + ".json", version,
            Pattern.quote("${" + versionProperty + "}"));
        assert template != null && template.length() > 0;
        return template.getBytes(StandardCharsets.UTF_8);
    }
}
