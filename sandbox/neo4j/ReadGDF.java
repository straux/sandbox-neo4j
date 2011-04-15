package sandbox.neo4j;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Arrays;

public class ReadGDF {
    public static final String CSV_PATTERN = "\"([^\"]+?)\",?|([^,]+),?|,";
    private static Pattern csvRE;

    protected int nb_lines = 0;
    protected Scanner scanner;
    Hashtable nodes;
    Vector nodes_order;
    Hashtable edges;
    Vector edges_order;

    ReadGDF( String file ) {
        csvRE = Pattern.compile(CSV_PATTERN);
        nodes = new Hashtable();
        nodes_order = new Vector( 25, 25 );
        edges = new Hashtable();
        edges_order = new Vector( 25, 25 );

        try {
            File source = new File( file );
            if( source.isFile() && source.getAbsolutePath().endsWith(".gdf") ) {
                scanner = new Scanner(source);
            } else {
                throw new IOException( "File '" + file + "' must be a .gdf file." );
            }
        } catch( IOException e ) {
           System.err.println( e );
           System.exit(0);
        }
    }

    public Boolean hasNextLine() {
        return scanner.hasNextLine();
    }

    protected Vector getLine() {
        Vector fields = new Vector( 25, 25 );
        String line = scanner.nextLine();
        nb_lines++;

        Matcher m = csvRE.matcher(line);
        while (m.find()) {
            String match = m.group();
            if (match == null) {
                break;
            }
            if (match.endsWith(",")) { // trim trailing ,
                match = match.substring(0, match.length() - 1);
            }
            if (match.startsWith("\"")) {
                if(!match.endsWith("\"")){
                    System.err.println( "quotation error line " + nb_lines );
                }
                match = match.substring(1, match.length() - 1);
            }
            if (match.length() == 0) {
                match = null;
            }
            fields.add(match);
        }
        return fields;
    }

    protected void parse_nodes() {
       Vector line = getLine();
       if( ((String) line.get( 0 )).toLowerCase().startsWith( "nodedef> ", 0 ) ) {
           Enumeration elt = line.elements();
           int first = 1;
            while( elt.hasMoreElements() ) {
                String e = (String) elt.nextElement();
                String [] head = e.split(" ");
                if( first == 1 ) {
                    add_field( 1, head, nodes, nodes_order );
                    first = 0;
                } else {
                    add_field( 0, head, nodes, nodes_order );
                }
            }
        } else {
           System.err.println("File must begin with 'nodedef> ': got " + line.get( 0 ) + " instead.");
           System.exit(0);
        }
    }

    protected Boolean check_edgedef( String field ) {
       return field.toLowerCase().startsWith( "edgedef> ", 0 );
    }

    public Hashtable get_node() {
       if( nodes_order.size() == 0 ) {
           parse_nodes();
       }

       Vector line = getLine();
       Hashtable fields = new Hashtable();
       if( check_edgedef( (String) line.get( 0 ) ) ) {
            parse_edges( line );
        } else {
           parse_line( line, fields, nodes, nodes_order );
        }
       return fields;
    }

    public Hashtable get_edge() {
       if( edges_order.size() == 0 ) {
           parse_edges();
       }

       Vector line = getLine();
       Hashtable fields = new Hashtable();
       parse_line( line, fields, edges, edges_order );
       return fields;
    }

    protected void parse_line( Vector line, Hashtable fields, Hashtable types, Vector order ) {
        for( int i = 0; i < line.size(); i++ ) {
            String val = (String) line.get( i );
            String key = (String) order.get( i );
            String type = (String) types.get( key );
            Object o;
            if( type.equals( "INT" ) ) {
                o = Integer.parseInt( val );
            } else if( type.equals( "DOUBLE" ) ) {
                o = Double.parseDouble( val );
            } else if( type.equals( "BOOLEAN" ) ) {
                o = Boolean.parseBoolean( val );
            } else if( type.equals( "VARCHAR" ) ) {
                o = val;
            } else {
                System.err.println("unknown type: " + type );
                o = val;
            }
            fields.put( key, o );
        }
    }

    protected void parse_edges() {
       Vector line = getLine();
       if( ((String) line.get( 0 )).toLowerCase().startsWith( "edgedef> ", 0 ) ) {
            parse_edges( line );
        } else {
           System.err.println("edges must begin with 'edgedef> ': got " + line.get( 0 ) + " instead.");
           System.exit(0);
        }
    }

    protected void parse_edges( Vector line ) {
           Enumeration elt = line.elements();
           int first = 1;
            while( elt.hasMoreElements() ) {
                String e = (String) elt.nextElement();
                String [] head = e.split(" ");
                if( first == 1 ) {
                    add_field( 1, head, edges, edges_order );
                    first = 0;
                } else {
                    add_field( 0, head, edges, edges_order );
                }
            }

    }
    
    protected void add_field( int offset, String [] head, Hashtable types, Vector order ) {
        String t = "VARCHAR";

        if( head.length > (offset+1) ) {
            t = head[ offset + 1].toUpperCase(); 
        }

        types.put( head[ offset ], t );
        order.add( head[ offset ] );
    }
}

