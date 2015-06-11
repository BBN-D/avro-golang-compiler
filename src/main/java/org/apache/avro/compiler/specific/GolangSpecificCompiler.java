/**
 * Copyright (c) 2014 Raytheon BBN Technologies Corp
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.specific;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonNode;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeServices;
import org.apache.velocity.runtime.log.LogChute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate specific golang interfaces and classes for protocols and schemas.
 *
 * golang reserved keywords are mangled to preserve compilation.
 */
public class GolangSpecificCompiler {

  private final Set<Schema> queue = new HashSet<Schema>();
  private Protocol protocol;
  private VelocityEngine velocityEngine;
  private String templateDir;
  private String outputCharacterEncoding;
  private String outputPackageImportName;	// eg, github.com/username/foo
  private String outputPackageName;			// eg, foo
  private String avroPackageImportName;		// eg, github.com/username/foo
  private String avroPackage;				// eg, foo

  /* List of reserved words from
   * http://java.sun.com/docs/books/jls/third_edition/html/lexical.html. */
  private static final Set<String> RESERVED_WORDS = new HashSet<String>(
      Arrays.asList(new String[] {
          "break", "default", "func", "interface", "select",
          "case", "defer", "go", "map", "struct",
          "chan", "else", "goto", "package", "switch",
          "const", "fallthrough", "if", "range", "type",
          "continue", "for", "import", "return", "var"
        }));
  
  private static final String FILE_HEADER =
      "/**\n" +
      " * Autogenerated by Avro\n" +
      " * \n" +
      " * DO NOT EDIT DIRECTLY\n" +
      " */\n";
  
  public GolangSpecificCompiler(Protocol protocol) {
    this();
    // enqueue all types
    for (Schema s : protocol.getTypes()) {
      enqueue(s);
    }
    this.protocol = protocol;
  }

  public GolangSpecificCompiler(Schema schema) {
    this();
    enqueue(schema);
    this.protocol = null;
  }
  
  GolangSpecificCompiler() {
    this.templateDir =
      System.getProperty("org.apache.avro.specific.templates",
                         "/org/apache/avro/compiler/specific/templates/golang/classic/");
    initializeVelocity();
  }

  /** Set the resource directory where templates reside. First, the compiler checks
   * the system path for the specified file, if not it is assumed that it is
   * present on the classpath.*/
  public void setTemplateDir(String templateDir) {
    this.templateDir = templateDir;
  }
  
  /** Set the full golang output package name for imports
   *  eg, github.com/myname/myavro
   */
  public void setOutputPackageName(String outputPackageImportName) {
	  this.outputPackageImportName = outputPackageImportName;
	  
	  if (outputPackageImportName.lastIndexOf("/") != -1)
		  this.outputPackageName = outputPackageImportName.substring(outputPackageImportName.lastIndexOf("/") + 1);
	  else
		  this.outputPackageName = outputPackageImportName;
	  
	  this.avroPackageImportName = outputPackageImportName;
	  this.avroPackage = this.outputPackageName + ".";
  }
  

  private static String logChuteName = null;

  private void initializeVelocity() {
    this.velocityEngine = new VelocityEngine();

    // These  properties tell Velocity to use its own classpath-based
    // loader, then drop down to check the root and the current folder
    velocityEngine.addProperty("resource.loader", "class, file");
    velocityEngine.addProperty("class.resource.loader.class",
        "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
    velocityEngine.addProperty("file.resource.loader.class", 
        "org.apache.velocity.runtime.resource.loader.FileResourceLoader");
    velocityEngine.addProperty("file.resource.loader.path", "/, .");
    velocityEngine.setProperty("runtime.references.strict", true);
    
    // allows use of the $macro directive
    velocityEngine.setProperty("macro.provide.scope.control", true);
    
    // allows ridiculous macro expansion
    velocityEngine.setProperty("velocimacro.max.depth", 256);

    // try to use Slf4jLogChute, but if we can't use the null one.
    if (null == logChuteName) {
      // multiple threads can get here concurrently, but that's ok.
      try {
        new Slf4jLogChute();
        logChuteName = Slf4jLogChute.class.getName();
      } catch (Exception e) {
        logChuteName = "org.apache.velocity.runtime.log.NullLogChute";
      }
    }
    velocityEngine.setProperty("runtime.log.logsystem.class", logChuteName);
  }

  /**
   * Captures output file path and contents.
   */
  static class OutputFile {
    String path;
    String contents;
    String outputCharacterEncoding;

    /**
     * Writes output to path destination directory when it is newer than src,
     * creating directories as necessary.  Returns the created file.
     */
    File writeToDestination(File src, File destDir) throws IOException {
      File f = new File(destDir, path);
      if (src != null && f.exists() && f.lastModified() >= src.lastModified())
        return f;                                 // already up to date: ignore
      f.getParentFile().mkdirs();
      Writer fw;
      if (outputCharacterEncoding != null) {
        fw = new OutputStreamWriter(new FileOutputStream(f), outputCharacterEncoding);
      } else {
        fw = new FileWriter(f);
      }
      try {
        fw.write(FILE_HEADER);
        fw.write(contents);
      } finally {
        fw.close();
      }
      return f;
    }
  }

  /**
   * Generates Java interface and classes for a protocol.
   * @param src the source Avro protocol file
   * @param dest the directory to place generated files in
   */
  public static void compileProtocol(File src, File dest) throws IOException {
    compileProtocol(new File[] {src}, dest);
  }

  /**
   * Generates Java interface and classes for a number of protocol files.
   * @param srcFiles the source Avro protocol files
   * @param dest the directory to place generated files in
   */
  public static void compileProtocol(File[] srcFiles, File dest) throws IOException {
    for (File src : srcFiles) {
      Protocol protocol = Protocol.parse(src);
      GolangSpecificCompiler compiler = new GolangSpecificCompiler(protocol);
      compiler.compileToDestination(src, dest);
    }
  }

  /** Generates Java classes for a schema. */
  public static void compileSchema(File src, File dest) throws IOException {
    compileSchema(new File[] {src}, dest);
  }

  /** Generates Java classes for a number of schema files. */
  public static void compileSchema(File[] srcFiles, File dest) throws IOException {
    Schema.Parser parser = new Schema.Parser();

    for (File src : srcFiles) {
      Schema schema = parser.parse(src);
      GolangSpecificCompiler compiler = new GolangSpecificCompiler(schema);
      compiler.compileToDestination(src, dest);
    }
  }

  /** Recursively enqueue schemas that need a class generated. */
  private void enqueue(Schema schema) {
    if (queue.contains(schema)) return;
    switch (schema.getType()) {
    case RECORD:
      queue.add(schema);
      for (Schema.Field field : schema.getFields())
        enqueue(field.schema());
      break;
    case MAP:
      enqueue(schema.getValueType());
      break;
    case ARRAY:
      enqueue(schema.getElementType());
      break;
    case UNION:
      for (Schema s : schema.getTypes())
        enqueue(s);
      break;
    case ENUM:
    case FIXED:
      queue.add(schema);
      break;
    case STRING: case BYTES:
    case INT: case LONG:
    case FLOAT: case DOUBLE:
    case BOOLEAN: case NULL:
      break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }

  /** Generate golang classes for enqueued schemas. */
  Collection<OutputFile> compile(String postfix) {
    List<OutputFile> out = new ArrayList<OutputFile>();
    for (Schema schema : queue) {
      out.add(compile(schema, postfix));
    }
    if (protocol != null) {
      //out.add(compileInterface(protocol));
    }
    return out;
  }

  /** Generate output under dst, unless existing file is newer than src. */
  public void compileToDestination(File src, File dst) throws IOException {
    for (Schema schema : queue) {
      OutputFile o = compile(schema, "");
      o.writeToDestination(src, dst);
    }
    if (protocol != null) {
      //compileInterface(protocol).writeToDestination(src, dst);
    }
  }
  
  /** Generate output under dst, unless existing file is newer than src. */
  public void compileTestsToDestination(File src, File dst) throws IOException {
    for (Schema schema : queue) {
      OutputFile o = compile(schema, "_test");
      o.writeToDestination(src, dst);
    }
    if (protocol != null) {
      //compileInterface(protocol).writeToDestination(src, dst);
    }
  }
  
  public void addToContext(VelocityContext context) {
	  context.put("this", this);
	  context.put("outputPackageName", outputPackageName);
	  context.put("avroPackage", avroPackage);
	  
	  //RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES,
      //INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;
	  
	  context.put("RECORD", Schema.Type.RECORD);
	  context.put("ENUM", Schema.Type.ENUM);
	  context.put("ARRAY", Schema.Type.ARRAY);
	  context.put("MAP", Schema.Type.MAP);
	  context.put("UNION", Schema.Type.UNION);
	  context.put("FIXED", Schema.Type.FIXED);
	  context.put("BYTES", Schema.Type.BYTES);
	  context.put("STRING", Schema.Type.STRING);
	  context.put("INT", Schema.Type.INT);
	  context.put("LONG", Schema.Type.LONG);
	  context.put("FLOAT", Schema.Type.FLOAT);
	  context.put("DOUBLE", Schema.Type.DOUBLE);
	  context.put("BOOLEAN", Schema.Type.BOOLEAN);
	  context.put("NULL", Schema.Type.NULL);
  }
  
  public OutputFile compileLibFile(String libFilename)
  {
    VelocityContext context = new VelocityContext();
    addToContext(context);
    
    OutputFile outputFile = new OutputFile();
    outputFile.path = libFilename;
    outputFile.contents = renderTemplate(templateDir+libFilename, context);
    outputFile.outputCharacterEncoding = outputCharacterEncoding;
    return outputFile;
  }
  
  public void copyGolangLibFiles(File dst) throws IOException
  {
	  compileLibFile("avro.go").writeToDestination(null, dst);
	  compileLibFile("datafile.go").writeToDestination(null, dst);
	  compileLibFile("memopen.go").writeToDestination(null, dst);
  }

  private String renderTemplate(String templateName, VelocityContext context) {
    Template template;
    try {
      template = this.velocityEngine.getTemplate(templateName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    StringWriter writer = new StringWriter();
    template.merge(context, writer);
    return writer.toString();
  }

  OutputFile compileInterface(Protocol protocol) {
    protocol = addStringType(protocol);           // annotate protocol as needed
    VelocityContext context = new VelocityContext();
    context.put("protocol", protocol);
    addToContext(context);
    String out = renderTemplate(templateDir+"protocol.vm", context);

    OutputFile outputFile = new OutputFile();
    String mangledName = mangle(protocol.getName());
    outputFile.path = makePath(mangledName, protocol.getNamespace());
    outputFile.contents = out;
    outputFile.outputCharacterEncoding = outputCharacterEncoding;
    return outputFile;
  }

  static String makePath(String name, String space) {
    if (space == null || space.isEmpty()) {
      return name + ".go";
    } else {
      return space.replace('.', File.separatorChar) + File.separatorChar + name
          + ".go";
    }
  }

  OutputFile compile(Schema schema, String postfix) {
    schema = addStringType(schema);               // annotate schema as needed
    String output = "";
    VelocityContext context = new VelocityContext();
    context.put("schema", schema);
    addToContext(context);
    
    OutputFile outputFile = new OutputFile();
    String name = mangle(schema.getName());

    switch (schema.getType()) {
    case RECORD:
      output = renderTemplate(templateDir+"record" + postfix + ".vm", context);
      name = name + postfix;
      break;
    case ENUM:
      output = renderTemplate(templateDir+"enum.vm", context);
      break;
    case FIXED:
      output = renderTemplate(templateDir+"fixed.vm", context);
      break;
    case BOOLEAN:
    case NULL:
      break;
    default: throw new RuntimeException("Unknown type: "+schema);
    }
    
    outputFile.path = makePath(name, schema.getNamespace());
    outputFile.contents = output;
    outputFile.outputCharacterEncoding = outputCharacterEncoding;
    return outputFile;
  }

  private StringType stringType = StringType.CharSequence;

  /** Set the Java type to be emitted for string schemas. */
  public void setStringType(StringType t) { this.stringType = t; }

  // annotate map and string schemas with string type
  private Protocol addStringType(Protocol p) {
    if (stringType != StringType.String)
      return p;

    Protocol newP = new Protocol(p.getName(), p.getDoc(), p.getNamespace());
    Map<Schema,Schema> types = new LinkedHashMap<Schema,Schema>();

    // Copy properties
    for (Map.Entry<String,JsonNode> prop : p.getJsonProps().entrySet())
      newP.addProp(prop.getKey(), prop.getValue());   // copy props

    // annotate types
    Collection<Schema> namedTypes = new LinkedHashSet<Schema>();
    for (Schema s : p.getTypes())
      namedTypes.add(addStringType(s, types));
    newP.setTypes(namedTypes);

    // annotate messages
    Map<String,Message> newM = newP.getMessages();
    for (Message m : p.getMessages().values())
      newM.put(m.getName(), m.isOneWay()
               ? newP.createMessage(m.getName(), m.getDoc(), m.getJsonProps(),
                                    addStringType(m.getRequest(), types))
               : newP.createMessage(m.getName(), m.getDoc(), m.getJsonProps(),
                                    addStringType(m.getRequest(), types),
                                    addStringType(m.getResponse(), types),
                                    addStringType(m.getErrors(), types)));
    return newP;
  }

  private Schema addStringType(Schema s) {
    if (stringType != StringType.String)
      return s;
    return addStringType(s, new LinkedHashMap<Schema,Schema>());
  }

  // annotate map and string schemas with string type
  private Schema addStringType(Schema s, Map<Schema,Schema> seen) {
    if (seen.containsKey(s)) return seen.get(s); // break loops
    Schema result = s;
    switch (s.getType()) {
    case STRING:
      result = Schema.create(Schema.Type.STRING);
      GenericData.setStringType(result, stringType);
      break;
    case RECORD:
      result =
        Schema.createRecord(s.getFullName(), s.getDoc(), null, s.isError());
      for (String alias : s.getAliases())
        result.addAlias(alias, null);             // copy aliases
      seen.put(s, result);
      List<Field> newFields = new ArrayList<Field>();
      for (Field f : s.getFields()) {
        Schema fSchema = addStringType(f.schema(), seen);
        Field newF =
          new Field(f.name(), fSchema, f.doc(), f.defaultValue(), f.order());
        for (Map.Entry<String,JsonNode> p : f.getJsonProps().entrySet())
          newF.addProp(p.getKey(), p.getValue()); // copy props
        for (String a : f.aliases())
          newF.addAlias(a);                       // copy aliases
        newFields.add(newF);
      }
      result.setFields(newFields);
      break;
    case ARRAY:
      Schema e = addStringType(s.getElementType(), seen);
      result = Schema.createArray(e);
      break;
    case MAP:
      Schema v = addStringType(s.getValueType(), seen);
      result = Schema.createMap(v);
      GenericData.setStringType(result, stringType);
      break;
    case UNION:
      List<Schema> types = new ArrayList<Schema>();
      for (Schema branch : s.getTypes())
        types.add(addStringType(branch, seen));
      result = Schema.createUnion(types);
      break;
    }
    for (Map.Entry<String,JsonNode> p : s.getJsonProps().entrySet())
      result.addProp(p.getKey(), p.getValue());   // copy props
    seen.put(s, result);
    return result;
  }

  public void raiseError(String message) throws Exception {
	  throw new RuntimeException(message);
  }
  
  /** Utility for template use */
  public String addPrefix(String prefix, String str) {
	  return prefix + str.replace("\n", "\n" + prefix);
  }
  
  
  /** Utility for template use. Returns the golang package name for a Schema */
  public String golangPackageName(Schema schema) {
	if (schema.getNamespace().lastIndexOf('.') != -1)
		return mangle(schema.getNamespace().substring(schema.getNamespace().lastIndexOf('.') + 1));
	else if (schema.getNamespace().isEmpty())
		return outputPackageName;
	else
		return mangle(schema.getNamespace());
  }
  
  /** Utility for template use.  Returns the golang type for a Schema. */
  public String golangType(Schema schema, Schema fieldSchema) {
	  return golangTypeImpl(schema, fieldSchema, false);
  }
  
  public String golangTypeDecl(Schema schema, Schema fieldSchema) {
	  return golangTypeImpl(schema, fieldSchema, true);
  }
	  
  public String golangTypeImpl(Schema schema, Schema fieldSchema, boolean decl) {
    switch (fieldSchema.getType()) {
    case ENUM:
    	decl = false;
    	// intentionally fall through
    case RECORD:
    case FIXED:
      String prefix = decl ? "*" : "";
      if (fieldSchema.getNamespace().equals(schema.getNamespace()))
        return prefix + mangle(fieldSchema.getName());
      else
        return prefix + golangPackageName(fieldSchema) + "." + mangle(fieldSchema.getName());
    case ARRAY:
      return "[]" + golangTypeDecl(schema, fieldSchema.getElementType());
    case MAP:
      return "map[string]" + golangTypeDecl(schema, fieldSchema.getValueType());
    case UNION:
      return "interface{}";
    case STRING:  return "string";
    case BYTES:   return "[]byte";
    case INT:     return "int32";
    case LONG:    return "int64";
    case FLOAT:   return "float32";
    case DOUBLE:  return "float64";
    case BOOLEAN: return "bool";
    case NULL:    return "interface{}";
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }
  
  public String golangTypeDescription(Field field)
  {
	  if (field.schema().getType() == Schema.Type.UNION) {
		  String ret = "/* union types: ";
		  boolean first = true;
		  
		  for (Schema type: field.schema().getTypes()) {
			  if (first) {
				  first = false;
			  } else {
				ret += ", ";  
			  }
			  
			  try {
				  ret += type.getNamespace() + ".";
			  } catch (Exception e){
			  }
			  
			  ret += type.getName();
		  }
		  
		  return ret + " */";  
	  }
	  return "";
  }

  // maximum size for string constants, to avoid javac limits
  int maxStringChars = 8192;

  /** Utility for template use. Takes a (potentially overly long) string and
   *  splits it into a quoted, comma-separted sequence of escaped strings.
   *  @param s The string to split
   *  @return A sequence of quoted, comma-separated, escaped strings
   */
  public String javaSplit(String s) throws IOException {
    StringBuilder b = new StringBuilder("\"");    // initial quote
    for (int i = 0; i < s.length(); i += maxStringChars) {
      if (i != 0) b.append("\",\"");              // insert quote-comma-quote
      String chunk = s.substring(i, Math.min(s.length(), i + maxStringChars));
      b.append(javaEscape(chunk));                // escape chunks
    }
    b.append("\"");                               // final quote
    return b.toString();
  }
  
  /** Utility for template use.  Escapes quotes and backslashes. */
  public static String javaEscape(Object o) {
      return o.toString().replace("\\","\\\\").replace("\"", "\\\"");
  }

  /** Utility for template use.  Escapes comment end with HTML entities. */
  public static String escapeForJavadoc(String s) {
      return s.replace("*/", "*&#47;");
  }
  
  /** Utility for template use.  Returns empty string for null. */
  public static String nullToEmpty(String x) {
    return x == null ? "" : x;
  }

  /** Utility for template use.  Adds a dollar sign to reserved words. */
  public static String mangle(String word) {
    return mangle(word, false);
  }
  
  /** Utility for template use.  Adds a dollar sign to reserved words. */
  public static String mangle(String word, boolean isError) {
    return mangle(word, RESERVED_WORDS);
  }
  
  /** Utility for template use.  Adds a dollar sign to reserved words. */
  public static String mangle(String word, Set<String> reservedWords) {
    return mangle(word, reservedWords, false);
  }
  
  /** Utility for template use.  Adds an underscore to reserved words. */
  public static String mangle(String word, Set<String> reservedWords, 
      boolean isMethod) {
    if (reservedWords.contains(word) || 
        (isMethod && reservedWords.contains(
            Character.toLowerCase(word.charAt(0)) + 
            ((word.length() > 1) ? word.substring(1) : "")))) {
      return word + "_";
    }
    return word;
  }
  
  /** Utility for template use */
  public static String generateFieldName(Field field, Schema schema)
  {
	  String name = Character.toUpperCase(field.name().charAt(0)) + field.name().substring(1);
	  return mangle(name, schema.isError());
  }
  
  private String getTypeImportName(Schema baseSchema, Schema fieldSchema)
  {
	  switch (fieldSchema.getType())
	  {
	  case RECORD:
	  case FIXED:
	  case ENUM:
		  if (!fieldSchema.getNamespace().equals(baseSchema.getNamespace()))
			return mangle(fieldSchema.getNamespace()).replace(".", "/");
		  break;
	  case ARRAY:
		  return getTypeImportName(baseSchema, fieldSchema.getElementType()); 
	  case MAP:
		  return getTypeImportName(baseSchema, fieldSchema.getValueType());
	  case UNION:
	      return null;
	  default:
		  break;
	  }
	  return null;
  }
  
    
  public Set<String> getImports(Schema schema) {
	  Set<String> imports = new HashSet<String>();
	  
	  if (!schema.getNamespace().isEmpty())
		  imports.add(outputPackageImportName);
	  
	  
	  if (schema.getType() == Schema.Type.RECORD)
		  for (Field field: schema.getFields()) {
			  String name = getTypeImportName(schema, field.schema());
			  if (name != null)
				  imports.add(outputPackageImportName + "/" + name);
		  }
	  
	  return imports;
  }
  
  /** Utility for template use */
  public Set<String> getTestImports(Schema baseSchema)
  {
	  return getTestImports(baseSchema, baseSchema);
  }
  
  protected Set<String> getTestImports(Schema baseSchema, Schema fieldSchema) {
	  Set<String> imports = new HashSet<String>();
	  
	  String name = getTypeImportName(baseSchema, fieldSchema);
	  if (name != null)
		  imports.add(outputPackageImportName + "/" + name);
	  
	  switch (fieldSchema.getType()) {
	  case RECORD:
		  for (Field field: fieldSchema.getFields()) {
			  imports.addAll(getTestImports(baseSchema, field.schema()));
		  }
		  break;
	  case ARRAY:
		  imports.addAll(getTestImports(baseSchema, fieldSchema.getElementType()));
		  break;
	  case MAP:
		  imports.addAll(getTestImports(baseSchema, fieldSchema.getValueType()));
		  break;
	  case UNION:
		  for (Schema fs: fieldSchema.getTypes()) {
			  imports.addAll(getTestImports(baseSchema, fs));  
		  }
		  break;
	  default:
		  break;
	  }
	  
	  return imports;
  }
  
  
  /** Utility for template use */
  public String getSchemaGoType(Schema schema, Schema fieldSchema) {
    switch (fieldSchema.getType()) {
    case RECORD:
    case FIXED:
    case ENUM:
      if (fieldSchema.getNamespace().equals(schema.getNamespace()))
        return mangle(fieldSchema.getName()) + "Schema";
      else
        return golangPackageName(fieldSchema) + "." + mangle(fieldSchema.getName()) + "Schema";
    case ARRAY:
    case MAP:
    case UNION:
    	return "XXX";// these shouldn't call this
    case STRING:  return avroPackage + "StringSchema";
    case BYTES:   return avroPackage + "BytesSchema";
    case INT:     return avroPackage + "IntSchema";
    case LONG:    return avroPackage + "LongSchema";
    case FLOAT:   return avroPackage + "FloatSchema";
    case DOUBLE:  return avroPackage + "DoubleSchema";
    case BOOLEAN: return avroPackage + "BooleanSchema";
    case NULL:    return avroPackage + "NullSchema";
    default: throw new RuntimeException("Unknown type: "+schema);
    }
  }
  
  /** Utility for template use, because go can't do uninitialized vars */
  public boolean toRecordNeedsError(Schema schema) {
	  for (Field f: schema.getFields()) {
		  if (f.schema().getType() != Schema.Type.ARRAY && f.schema().getType() != Schema.Type.MAP)
			  return true;
	  }
	  return false;
  }
  
  /** Utility for template use */
  public int testRuns(Schema schema) {
	  // returns that max number of unions, so we hit each branch. insane.
	  
	  int runs = 1;
	  
	  switch (schema.getType())
	  {
	  case RECORD:
		  for (Field field: schema.getFields())
			  runs = Math.max(runs, testRuns(field.schema()));
		  break;
	  case UNION:
		  runs = Math.max(runs, schema.getTypes().size());
		  break;
	  case MAP:
		  runs = Math.max(runs, testRuns(schema.getValueType()));
		  break;
	  case ARRAY:
		  runs = Math.max(runs, testRuns(schema.getElementType()));
		  break;
	  default:
		  break;
	  }
	  
	  return runs;
  }
  
  public void printit(String s) { System.out.println(s); }
  

  public static void main(String[] args) throws Exception {
    //compileSchema(new File(args[0]), new File(args[1]));
    compileProtocol(new File(args[0]), new File(args[1]));
  }
  
  public static final class Slf4jLogChute implements LogChute {
    private Logger logger = LoggerFactory.getLogger("AvroVelocityLogChute");
    
    public void init(RuntimeServices rs) throws Exception {
      // nothing to do
    }

    public void log(int level, String message) {
      switch (level) {
      case LogChute.DEBUG_ID:
        logger.debug(message);
        break;
      case LogChute.TRACE_ID:
        logger.trace(message);
        break;
      case LogChute.WARN_ID:
        logger.warn(message);
        break;
      case LogChute.ERROR_ID:
        logger.error(message);
        break;
      default:
      case LogChute.INFO_ID:
        logger.info(message);
        break;
      }
    }

    public void log(int level, String message, Throwable t) {
      switch (level) {
      case LogChute.DEBUG_ID:
        logger.debug(message, t);
        break;
      case LogChute.TRACE_ID:
        logger.trace(message, t);
        break;
      case LogChute.WARN_ID:
        logger.warn(message, t);
        break;
      case LogChute.ERROR_ID:
        logger.error(message, t);
        break;
      default:
      case LogChute.INFO_ID:
        logger.info(message, t);
        break;
      }
    }

    public boolean isLevelEnabled(int level) {
      switch (level) {
      case LogChute.DEBUG_ID:
        return logger.isDebugEnabled();
      case LogChute.TRACE_ID:
        return logger.isTraceEnabled();
      case LogChute.WARN_ID:
        return logger.isWarnEnabled();
      case LogChute.ERROR_ID:
        return logger.isErrorEnabled();
      default:
      case LogChute.INFO_ID:
        return logger.isInfoEnabled();
      }
    }
  }

  /** Sets character encoding for generated java file
  * @param outputCharacterEncoding Character encoding for output files (defaults to system encoding)
  */
  public void setOutputCharacterEncoding(String outputCharacterEncoding) {
    this.outputCharacterEncoding = outputCharacterEncoding;
  }
}
