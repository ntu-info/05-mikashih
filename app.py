# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        """Get studies that mention a specific term."""
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Query studies that mention this term
                rows = conn.execute(text("""
                    SELECT DISTINCT study_id, term, AVG(weight) as avg_weight
                    FROM ns.annotations_terms
                    WHERE term LIKE :term
                    GROUP BY study_id, term
                    ORDER BY avg_weight DESC
                    LIMIT 100;
                """), {"term": f"%{term}%"}).mappings().all()
                
                studies = [dict(r) for r in rows]
                return jsonify({
                    "term": term,
                    "count": len(studies),
                    "studies": studies
                }), 200
                
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        """Get studies at specific MNI coordinates."""
        try:
            x, y, z = map(float, coords.split("_"))
        except ValueError:
            return jsonify({"error": "Invalid coordinates format. Use x_y_z"}), 400
            
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Find studies within 5mm radius of the coordinate
                rows = conn.execute(text("""
                    SELECT DISTINCT study_id, 
                           ST_X(geom) as x, 
                           ST_Y(geom) as y, 
                           ST_Z(geom) as z,
                           ST_Distance(geom, ST_SetSRID(ST_MakePoint(:x, :y, :z), 4326)) as distance
                    FROM ns.coordinates
                    WHERE ST_DWithin(geom, ST_SetSRID(ST_MakePoint(:x, :y, :z), 4326), 5)
                    ORDER BY distance
                    LIMIT 100;
                """), {"x": x, "y": y, "z": z}).mappings().all()
                
                studies = [dict(r) for r in rows]
                return jsonify({
                    "coordinates": {"x": x, "y": y, "z": z},
                    "count": len(studies),
                    "studies": studies
                }), 200
                
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_by_terms(term_a, term_b):
        """
        Functional dissociation by terms.
        Returns studies that mention term_a but NOT term_b.
        """
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Studies with term_a but NOT term_b
                rows = conn.execute(text("""
                    SELECT DISTINCT a.study_id, a.term, a.weight
                    FROM ns.annotations_terms a
                    WHERE a.term LIKE :term_a
                      AND NOT EXISTS (
                          SELECT 1 FROM ns.annotations_terms b
                          WHERE b.study_id = a.study_id
                            AND b.term LIKE :term_b
                      )
                    ORDER BY a.weight DESC
                    LIMIT 100;
                """), {"term_a": f"%{term_a}%", "term_b": f"%{term_b}%"}).mappings().all()
                
                studies = [dict(r) for r in rows]
                
                # Generate HTML table
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Functional Dissociation: {term_a} \\ {term_b}</title>
                    <style>
                        body {{
                            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                            max-width: 1200px;
                            margin: 40px auto;
                            padding: 20px;
                            background-color: #f5f5f5;
                        }}
                        .header {{
                            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                            color: white;
                            padding: 30px;
                            border-radius: 10px;
                            margin-bottom: 30px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                        }}
                        h1 {{
                            margin: 0 0 10px 0;
                            font-size: 28px;
                        }}
                        .info {{
                            font-size: 16px;
                            opacity: 0.9;
                        }}
                        table {{
                            width: 100%;
                            border-collapse: collapse;
                            background-color: white;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                            border-radius: 8px;
                            overflow: hidden;
                        }}
                        th {{
                            background-color: #667eea;
                            color: white;
                            padding: 15px;
                            text-align: left;
                            font-weight: 600;
                        }}
                        td {{
                            padding: 12px 15px;
                            border-bottom: 1px solid #e0e0e0;
                        }}
                        tr:hover {{
                            background-color: #f8f9fa;
                        }}
                        tr:last-child td {{
                            border-bottom: none;
                        }}
                        .count-badge {{
                            display: inline-block;
                            background-color: #764ba2;
                            color: white;
                            padding: 5px 15px;
                            border-radius: 20px;
                            font-size: 14px;
                            margin-top: 10px;
                        }}
                        .weight {{
                            font-weight: bold;
                            color: #667eea;
                        }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>🧠 Functional Dissociation by Terms</h1>
                        <div class="info">
                            <strong>Term A:</strong> {term_a}<br>
                            <strong>Term B:</strong> {term_b}<br>
                            <strong>Description:</strong> Studies mentioning '{term_a}' but NOT '{term_b}'
                        </div>
                        <div class="count-badge">Total Results: {len(studies)}</div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Study ID</th>
                                <th>Term</th>
                                <th>Weight</th>
                            </tr>
                        </thead>
                        <tbody>
                """
                
                for idx, study in enumerate(studies, 1):
                    html += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{study['study_id']}</td>
                                <td>{study['term']}</td>
                                <td class="weight">{study['weight']:.6f}</td>
                            </tr>
                    """
                
                html += """
                        </tbody>
                    </table>
                </body>
                </html>
                """
                
                return html, 200
                
        except Exception as e:
            return f"<h1>Error</h1><p>{str(e)}</p>", 500

    @app.get("/dissociate/locations/<coords_a>/<coords_b>", endpoint="dissociate_locations")
    def dissociate_by_locations(coords_a, coords_b):
        """
        Functional dissociation by MNI coordinates.
        Returns studies at coords_a but NOT at coords_b.
        """
        try:
            x1, y1, z1 = map(float, coords_a.split("_"))
            x2, y2, z2 = map(float, coords_b.split("_"))
        except ValueError:
            return "<h1>Error</h1><p>Invalid coordinates format. Use x_y_z</p>", 400
            
        eng = get_engine()
        try:
            with eng.begin() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                
                # Studies at coords_a (within 5mm) but NOT at coords_b (within 5mm)
                rows = conn.execute(text("""
                    SELECT DISTINCT c1.study_id,
                           ST_X(c1.geom) as x,
                           ST_Y(c1.geom) as y,
                           ST_Z(c1.geom) as z,
                           ST_Distance(c1.geom, ST_SetSRID(ST_MakePoint(:x1, :y1, :z1), 4326)) as dist_a
                    FROM ns.coordinates c1
                    WHERE ST_DWithin(c1.geom, ST_SetSRID(ST_MakePoint(:x1, :y1, :z1), 4326), 5)
                      AND NOT EXISTS (
                          SELECT 1 FROM ns.coordinates c2
                          WHERE c2.study_id = c1.study_id
                            AND ST_DWithin(c2.geom, ST_SetSRID(ST_MakePoint(:x2, :y2, :z2), 4326), 5)
                      )
                    ORDER BY dist_a
                    LIMIT 100;
                """), {"x1": x1, "y1": y1, "z1": z1, "x2": x2, "y2": y2, "z2": z2}).mappings().all()
                
                studies = [dict(r) for r in rows]
                
                # Generate HTML table
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Functional Dissociation: [{x1}, {y1}, {z1}] \\ [{x2}, {y2}, {z2}]</title>
                    <style>
                        body {{
                            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                            max-width: 1200px;
                            margin: 40px auto;
                            padding: 20px;
                            background-color: #f5f5f5;
                        }}
                        .header {{
                            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                            color: white;
                            padding: 30px;
                            border-radius: 10px;
                            margin-bottom: 30px;
                            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                        }}
                        h1 {{
                            margin: 0 0 10px 0;
                            font-size: 28px;
                        }}
                        .info {{
                            font-size: 16px;
                            opacity: 0.9;
                        }}
                        .coords {{
                            display: inline-block;
                            background-color: rgba(255,255,255,0.2);
                            padding: 5px 10px;
                            border-radius: 5px;
                            margin: 5px;
                        }}
                        table {{
                            width: 100%;
                            border-collapse: collapse;
                            background-color: white;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                            border-radius: 8px;
                            overflow: hidden;
                        }}
                        th {{
                            background-color: #f5576c;
                            color: white;
                            padding: 15px;
                            text-align: left;
                            font-weight: 600;
                        }}
                        td {{
                            padding: 12px 15px;
                            border-bottom: 1px solid #e0e0e0;
                        }}
                        tr:hover {{
                            background-color: #fff5f7;
                        }}
                        tr:last-child td {{
                            border-bottom: none;
                        }}
                        .count-badge {{
                            display: inline-block;
                            background-color: #f093fb;
                            color: white;
                            padding: 5px 15px;
                            border-radius: 20px;
                            font-size: 14px;
                            margin-top: 10px;
                        }}
                        .distance {{
                            font-weight: bold;
                            color: #f5576c;
                        }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>📍 Functional Dissociation by MNI Coordinates</h1>
                        <div class="info">
                            <strong>Coordinates A:</strong> 
                            <span class="coords">[{x1}, {y1}, {z1}]</span><br>
                            <strong>Coordinates B:</strong> 
                            <span class="coords">[{x2}, {y2}, {z2}]</span><br>
                            <strong>Description:</strong> Studies at [{x1}, {y1}, {z1}] but NOT at [{x2}, {y2}, {z2}]
                        </div>
                        <div class="count-badge">Total Results: {len(studies)}</div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Study ID</th>
                                <th>X</th>
                                <th>Y</th>
                                <th>Z</th>
                                <th>Distance from A (mm)</th>
                            </tr>
                        </thead>
                        <tbody>
                """
                
                for idx, study in enumerate(studies, 1):
                    html += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{study['study_id']}</td>
                                <td>{study['x']:.1f}</td>
                                <td>{study['y']:.1f}</td>
                                <td>{study['z']:.1f}</td>
                                <td class="distance">{study['dist_a']:.2f}</td>
                            </tr>
                    """
                
                html += """
                        </tbody>
                    </table>
                </body>
                </html>
                """
                
                return html, 200
                
        except Exception as e:
            return f"<h1>Error</h1><p>{str(e)}</p>", 500

    @app.get("/test_db", endpoint="test_db")
    
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    # Select a few columns if they exist; otherwise select a generic subset
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, weight FROM ns.annotations_terms LIMIT 3"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception as e:
            payload["error"] = str(e)
            return jsonify(payload), 500

    return app

# WSGI entry point (no __main__)
app = create_app()
