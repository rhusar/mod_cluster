# Contributing to mod_cluster

## Localization

Log messages and exception messages in the `core` module use [JBoss Logging Tools](https://github.com/jboss-logging/jboss-logging-tools) for internationalization.
The annotated interfaces are:

- `ModClusterLogger` (`@MessageLogger`) -- log messages
- `ModClusterMessages` (`@MessageBundle`) -- exception messages

Translation files are located in `core/src/main/resources/org/jboss/modcluster/`.
They follow the naming convention `<Interface>.i18n_<language>.properties` (e.g. `ModClusterLogger.i18n_cs.properties` for Czech).
The annotation processor picks up translation files at compile time and generates locale-specific implementation classes automatically.

### Testing localized output

To run with a specific locale, set the `JAVA_TOOL_OPTIONS` environment variable so that all JVM processes (including surefire forks) pick it up:

```bash
export JAVA_TOOL_OPTIONS="-Duser.language=cs"
mvn clean install
```

### Adding a new language

1. Create new properties files following the naming convention:
   - `ModClusterLogger.i18n_<language>.properties`
   - `ModClusterMessages.i18n_<language>.properties`
2. Place them in `core/src/main/resources/org/jboss/modcluster/`.
3. Translate all keys present in the English `.i18n.properties` files generated in `target/classes/`.
   Refer to the existing translations for the expected format.
4. Rebuild the `core` module -- the annotation processor will generate the locale-specific implementation classes.
5. See the translation notes in `ModClusterLogger.i18n_cs.properties` for glossary and conventions.
