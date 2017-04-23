package org.aksw.dbpedia.stats;

import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class Main {

    public static List<String> files = new ArrayList<>();

    private static BufferedReader scanner;

    private static Map<String, Integer> classes;

    private static Map<String, Integer> props;

    private static Map<String, Integer> propsLD;

    //private static Multimap<String, String> classesAndProperties = ArrayListMultimap.create();

    static String sparqlEndpoint = "http://dbpedia.org/sparql";

    static String queryString = "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
            "SELECT DISTINCT ?Class \n" +
            "WHERE { ?Class a owl:Class }";



    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure(); // configure log4j for jena

        int MAX_DATA_SIZE = 1024 * 1024;
        byte[] data = new byte[MAX_DATA_SIZE];

        File dir = new File("/home/gpublio/dbpedia-stats/logs/dbpedia-2016"); //logs path

        listFilesForFolder(dir);

        int totalQueries = 0; //query counter, just for information

        classes = new HashMap<String, Integer>();
        props = new HashMap<String, Integer>();;
        propsLD = new HashMap<String, Integer>();
        try {
            queryEndpoint(queryString, sparqlEndpoint);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // loop for each file found
        for (String file : files) {
            String[] extension = file.split("\\.");
            if (extension[extension.length - 1].compareTo("gz") != 0) { // file is not a log, ignore
                continue;
            }

            FileInputStream fis = null;
            try {
                String fullPath = dir + "/" + file;
                fis = new FileInputStream(fullPath);
            } catch (FileNotFoundException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            try {

                scanner = new BufferedReader(new InputStreamReader(new GZIPInputStream(fis))); //log files are gziped
                //File fileRead = new File(new InputStreamReader(new GZIPInputStream(fis)));
                String line = null;

                System.out.println("BEGIN READING FILE " + file);

                //List<String> lines = Files.readLines(file, Charsets.UTF_8);

                while ((line = scanner.readLine()) != null) {
                    //dealing with log lines
                    String[] logLine = line.split(" ");
                    String query;

                    try {
                        query = java.net.URLDecoder.decode(logLine[7], "UTF-8");
                    } catch (Exception e) {
                        continue;
                    }

                    //System.out.println(query);

                    //analyzing only sparql select queries requisitions
                    Pattern findSelect = Pattern.compile("(\\/sparql\\?).*(\\&query\\=)|&*(SELECT)");
                    //Matcher match1 = findSelect.matcher(query);
                    Pattern findPropertyLD = Pattern.compile("(^\\/property\\/.*)");
                    Matcher matchPropLD = findPropertyLD.matcher(query);
                    /*if (match1.find()) {
                        totalQueries = totalQueries + 1; //query found! +1

                        //System.out.println("#QUERY " + i + "#"); //for debugging
                        //System.out.println(query);

                        //the following regex excludes the /sparql? path and any other parameters, leaving only queries
                        query = query.replaceAll("(^\\/sparql\\?).*(\\&query\\=)|&.*=.*", "");

                        //now check whether a PREFIX is defined for DBpedia classes
                        Pattern findPrefix = Pattern.compile("PREFIX .*: <http://(www.)?dbpedia.org/ontology/?>");
                        Matcher match2 = findPrefix.matcher(query);
                        if (match2.find()) {
                            List<String> prefixes = tokenize(query, "PREFIX ", " <http://dbpedia.org/ontology");
                            String prefix = prefixes.toString().replaceAll("\\[|\\]", "");
                            query = query.replace(prefix, "http://dbpedia.org/ontology/");
                            query = query.replace("dbo:", "http://dbpedia.org/ontology/"); //default prefix in virtuoso endpoint
                        }
                        //find only the query itself, discarding now all prefixes
                        int selectPos = query.toLowerCase().indexOf("select");
                        //and remove some abusive queries over the endpoint that create outliers on data
                        Pattern findAbusiveQuery1 = Pattern.compile("OPTIONAL \\s*\\{ \\?city geo:lat \\?latitude; geo:long \\?longitude\\}");
                        Matcher matchAbusiveQuery1 = findAbusiveQuery1.matcher(query);
                        Pattern findAbusiveQuery2 = Pattern.compile("UNION \\s+\\{\\?airport <http://dbpedia\\.org/ontology/iataLocationIdentifier> \\?iata\\. \\} OPTIONAL \\{ \\?airport foaf:homepage \\?airport_home\\. \\}");
                        Matcher matchAbusiveQuery2 = findAbusiveQuery2.matcher(query);
                        Pattern findAbusiveQuery3 = Pattern.compile("\\(ID\\|id\\|Id\\|image\\|Image\\|gray\\|dorlands\\|wiki\\|lat\\|long\\|color\\|info\\|Info\\|homepage\\|map\\|Map\\|updated\\|Updated\\|logo\\|Logo\\|pushpin\\|label\\|Label\\|photo\\|Photo\\)");
                        Matcher matchAbusiveQuery3 = findAbusiveQuery3.matcher(query);
                        if (selectPos < 0 || matchAbusiveQuery1.find() || matchAbusiveQuery2.find() || matchAbusiveQuery3.find()) {
                            totalQueries = totalQueries - 1;
                            continue;
                        }
                        query = query.substring(selectPos, query.length());
                        String[] tokens = query.split(" ");
                        //use regex to match properties and classes inside the query
                        Pattern findClass = Pattern.compile("http://(www.)?dbpedia.org/ontology/[A-Z]");
                        Pattern findProperty = Pattern.compile("\\b(http://(www.)?dbpedia.org/ontology/[^A-Z]{1}[a-zA-Z0-9_]*)\\b|\\b(http://(www.)?dbpedia.org/property/[a-zA-Z0-9_]*)\\b");
                        //iterate over query terms
                        for (String token : tokens) {
                            if(token.isEmpty()) continue;
                            Matcher match3 = findClass.matcher(token);
                            Matcher match4 = findProperty.matcher(token);
                            if (match3.find()) {
                                String classRef = token.replaceAll("<|>| \\.|.$|\\)|\n", "");
                                if (classes.containsKey(classRef)) {
                                    classes.put(classRef, classes.get(classRef) + 1);
                                }
                            }
                            else if (match4.find()) {
                                String propRef = "";
                                try {
                                    propRef = match4.group();
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                                if (props.containsKey(propRef)) {
                                    props.put(propRef, props.get(propRef) + 1);
                                }
                                else if (!propRef.isEmpty()) props.put(propRef, 1);
                            } //end of else if
                        }   //end of for each
                    } //end of if match select
                    else */
                    if(matchPropLD.find()) { //find properties that were requested over LD
                        String propLD = query.replaceAll("(^\\/property\\/|\\/$|\\&.*|\\?.*)", "");
                        if (propsLD.containsKey(propLD)) {
                            propsLD.put(propLD, propsLD.get(propLD) + 1);
                        }
                        else if (!propLD.isEmpty()) propsLD.put(propLD, 1);
                    }
                } //end of while scanner.read

                System.out.println("END OF FILE " + file);

                scanner.close();
                fis.close();

            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        System.out.println("Process finished. Total of " + totalQueries + " analyzed queries.");

        try {
            //write separated output files for class and property usage
            //File fileOut = new File("/home/gpublio/dbpedia-stats/classes_usage.txt");
            //File fileOut2 = new File("/home/gpublio/dbpedia-stats/properties_usage.txt");
            File fileOut3 = new File("/home/gpublio/dbpedia-stats/propertiesLD_usage.txt");
            //FileOutputStream fos = new FileOutputStream(fileOut);
            //FileOutputStream fos2 = new FileOutputStream(fileOut2);
            FileOutputStream fos3 = new FileOutputStream(fileOut3);
            //PrintWriter pw = new PrintWriter(fos);
            //PrintWriter pw2 = new PrintWriter(fos2);
            PrintWriter pw3 = new PrintWriter(fos3);


            /*for (String cls : classes.keySet()) {
                int value = classes.get(cls);
                pw.println(cls + "," + value);
            }

            for (String prp : props.keySet()) {
                int value = props.get(prp);
                pw2.println(prp + "," + value);
            }*/

            for (String prp : propsLD.keySet()) {
                int value = propsLD.get(prp);
                pw3.println(prp + "," + value);
            }

            /*pw.flush();
            pw.close();
            fos.close();
            pw2.flush();
            pw2.close();
            fos2.close();*/
            pw3.flush();
            pw3.close();
            fos3.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //the next three methods exist for matching DBpedia ontology prefix and splitting queries to find classes references
    //many thanks to OVERFLOW, S. (2017) [http://stackoverflow.com/a/1016493/2896809]
    public static String escapeRegexp(String regexp) {
        String specChars = "\\$.*+?|()[]{}^";
        String result = regexp;
        for (int i = 0; i < specChars.length(); i++) {
            Character curChar = specChars.charAt(i);
            result = result.replaceAll(
                    "\\" + curChar,
                    "\\\\" + (i < 2 ? "\\" : "") + curChar); // \ and $ must have special treatment
        }
        return result;
    }

    public static List<String> findGroup(String content, String pattern, int group) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(content);
        List<String> result = new ArrayList<String>();
        while (m.find()) {
            result.add(m.group(group));
        }
        return result;
    }

    public static List<String> tokenize(String content, String firstToken, String lastToken) {
        String regexp = lastToken.length() > 1
                ? escapeRegexp(firstToken) + "(.*?)" + escapeRegexp(lastToken)
                : escapeRegexp(firstToken) + "([^" + lastToken + "]*)" + escapeRegexp(lastToken);
        return findGroup(content, regexp, 1);
    }


    // This method goes recursively inside a folder, generating its files list
    public static void listFilesForFolder(final File folder) {
        if (folder.listFiles() != null) {
            for (final File fileEntry : folder.listFiles()) {
                if (fileEntry.isDirectory()) {
                    listFilesForFolder(fileEntry);
                } else {
                    files.add(fileEntry.getName());
                }
            }
        }
    }

    //query dbpedia for class list
    public static void queryEndpoint(String paramQuery, String endpoint)
            throws Exception
    {
        // Create a Query with the given String
        Query query = QueryFactory.create(paramQuery);

        // Create the Execution Factory using the given Endpoint
        QueryExecution qexec = QueryExecutionFactory.sparqlService(
                endpoint, query);

        // Set Timeout
        ((QueryEngineHTTP)qexec).addParam("timeout", "10000");
        ((QueryEngineHTTP)qexec).addParam("default-graph-uri", "http://dbpedia.org");


        // Execute Query
        int iCount = 0;
        ResultSet rs = qexec.execSelect();
        while (rs.hasNext()) {
            // Get Result
            QuerySolution qs = rs.next();

            // Get Variable Names
            Iterator<String> itVars = qs.varNames();

            // Count
            iCount++;

            // Display Result
            while (itVars.hasNext()) {
                String var = itVars.next().toString();
                String val = qs.get(var).toString();

                classes.put(val, 0);
            }
        }
    } // End of Method: queryEndpoint()


}

