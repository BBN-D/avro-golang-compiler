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

package main;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.compiler.specific.GolangSpecificCompiler;
import org.apache.avro.generic.GenericData;

/**
 * Generate Java classes and interfaces from AvroIDL files (.avdl)
 * 
 * @goal idl-protocol
 * @requiresDependencyResolution runtime
 * @phase generate-sources
 * @threadSafe
 */
public class GenerateGolang {

  static protected String templateDirectory = "/org/apache/avro/compiler/specific/templates/golang/classic/";
  
  // for java interoperability
  static protected String stringType = "String";
  
  static public void doCompile(String filename, File sourceDirectory, File outputDirectory,
		  					   String outputPackageName) throws IOException {
	Idl parser = null;
	try {
      //List runtimeClasspathElements = project.getRuntimeClasspathElements();
      

      List<URL> runtimeUrls = new ArrayList<URL>();

      // Add the source directory of avro files to the classpath so that
      // imports can refer to other idl files as classpath resources
      runtimeUrls.add(sourceDirectory.toURI().toURL());

      // If runtimeClasspathElements is not empty values add its values to Idl path.
      /*if (runtimeClasspathElements != null && !runtimeClasspathElements.isEmpty()) {
        for (Object runtimeClasspathElement : runtimeClasspathElements) {
          String element = (String) runtimeClasspathElement;
          runtimeUrls.add(new File(element).toURI().toURL());
      }
      }*/

      URLClassLoader projPathLoader = new URLClassLoader
          (runtimeUrls.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
        parser = new Idl(new File(sourceDirectory, filename), projPathLoader);

      Protocol p = parser.CompilationUnit();
      String json = p.toString(true);
      Protocol protocol = Protocol.parse(json);
      GolangSpecificCompiler compiler = new GolangSpecificCompiler(protocol);
      compiler.setStringType(GenericData.StringType.valueOf(stringType));
      compiler.setTemplateDir(templateDirectory);
      compiler.setOutputPackageName(outputPackageName);
      compiler.copyGolangLibFiles(outputDirectory);
      compiler.compileToDestination(null, outputDirectory);
      compiler.compileTestsToDestination(null, outputDirectory);
      
    } catch (ParseException e) {
      throw new IOException(e);
    } finally {
      if (parser != null) parser.close();
    }
  }
  
  public static void doCompileDir(File inDir, File outDir, String outputPackageName) throws IOException
  {
    for (File inFile: inDir.listFiles())
    {
      if (inFile.isDirectory())
        doCompileDir(inFile, outDir, outputPackageName);
      else
        doCompile(inFile.getName(), inDir, outDir, outputPackageName);
    }
  }

  public static void main(String [] args) throws IOException
  {
	// input avdl files
	File inDir = new File(args[0]);
	File outDir = new File(args[1]);
	String outputPackageName = args[2];
	
	doCompileDir(inDir, outDir, outputPackageName);
  }
}

