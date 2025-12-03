import re

regex = re.compile(
    r'<component group="com.nvidia.cuvs" name="cuvs-.*?</component>\s*',
    re.MULTILINE | re.DOTALL,
)

with open("gradle/verification-metadata.xml", "r+") as f:
    text = f.read()
    text = regex.sub("", text)
    f.seek(0)
    f.truncate()
    f.write(text)
