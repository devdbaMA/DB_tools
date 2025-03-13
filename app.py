from flask import Flask, render_template, request, flash, redirect, url_for, send_file, jsonify, session
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SelectField, SubmitField, validators, IntegerField
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
import os
import sqlite3
from datetime import datetime
import paramiko
from functools import wraps
import shutil
import subprocess
import winreg
import urllib.request
import zipfile

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'your-secret-key-here')

# SQLite database for user authentication
SQLITE_DB = 'users.db'

# Add this near the top of the file, after the app configuration
FIRST_TIME_SETUP = True  # This will allow first user creation without DB

# Database configuration
DB_CONFIG = {
    'host': 'ep-patient-leaf-a2238y40-pooler.eu-central-1.aws.neon.tech',
    'port': 5432,
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_Tql8aY6hCGmZ'
}

SSH_PARAMS = {
    'hostname': os.getenv('SSH_HOST', 'localhost'),
    'username': os.getenv('SSH_USER', 'postgres'),
    'password': os.getenv('SSH_PASSWORD', 'postgres'),
    'default_backup_path': os.getenv('BACKUP_PATH', '/var/lib/postgresql/backups')
}

# Flag to disable SSH requirement
USE_SSH = os.getenv('USE_SSH', 'false').lower() == 'true'

# Add near the top with other configurations
POSTGRES_BIN_PATH = os.getenv('POSTGRES_BIN_PATH', None)  # Allow manual configuration through environment variable

# User authentication forms
class LoginForm(FlaskForm):
    username = StringField('Username', [validators.DataRequired()])
    password = PasswordField('Password', [validators.DataRequired()])
    submit = SubmitField('Login')

class RegistrationForm(FlaskForm):
    username = StringField('Username', [
        validators.DataRequired(),
        validators.Length(min=4, max=25)
    ])
    email = StringField('Email', [
        validators.DataRequired(),
        validators.Email()
    ])
    password = PasswordField('Password', [
        validators.DataRequired(),
        validators.Length(min=6),
        validators.EqualTo('confirm', message='Passwords must match')
    ])
    confirm = PasswordField('Confirm Password')
    submit = SubmitField('Register')

class BackupForm(FlaskForm):
    backup_method = SelectField('Backup Method', choices=[
        ('pg_dump', 'pg_dump (Logical Backup)'),
        ('pg_basebackup', 'pg_basebackup (Physical Backup)')
    ])
    backup_type = SelectField('Backup Type', choices=[
        ('full', 'Full Backup'),
        ('schema', 'Schema Only'),
        ('data', 'Data Only')
    ])
    db_name = SelectField('Database Name', choices=[])
    backup_location = StringField('Backup Location')
    file_name = StringField('File Name')
    file_type = SelectField('File Type', choices=[
        ('sql', '.sql (Plain SQL)'),
        ('custom', '.backup (Custom Format)'),
        ('tar', '.tar (Archive)'),
        ('directory', 'Directory Format')
    ])
    submit = SubmitField('Start Backup')

# Forms
class ConnectionForm(FlaskForm):
    name = StringField('Connection Name', [
        validators.DataRequired(),
        validators.Length(min=3, max=50)
    ])
    host = StringField('Database Host', [validators.DataRequired()])
    port = IntegerField('Database Port', [validators.DataRequired(), validators.NumberRange(min=1, max=65535)], default=5432)
    database = StringField('Database Name', [validators.DataRequired()])
    user_name = StringField('Database User', [validators.DataRequired()])
    password = PasswordField('Database Password', [validators.DataRequired()])
    test_connection = SubmitField('Test Connection')
    save = SubmitField('Save Connection')

class ConnectionSelectorForm(FlaskForm):
    connection = SelectField('Select Connection', [validators.DataRequired()])
    test = SubmitField('Test Connection')

def init_sqlite_db():
    """Initialize SQLite database for user authentication"""
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    
    # Create users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create connections table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS db_connections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            host TEXT NOT NULL,
            port INTEGER NOT NULL DEFAULT 5432,
            database TEXT NOT NULL,
            user_name TEXT NOT NULL,
            password TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id),
            UNIQUE (user_id, name)
        )
    """)
    
    conn.commit()
    conn.close()

def migrate_db_schema():
    """Migrate database schema to add new columns if needed"""
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        
        # Check if port column exists in db_connections table
        cur.execute("PRAGMA table_info(db_connections)")
        columns = [column[1] for column in cur.fetchall()]
        
        # Add port column if it doesn't exist
        if 'port' not in columns:
            print("Migrating database: Adding port column to db_connections table")
            cur.execute("ALTER TABLE db_connections ADD COLUMN port INTEGER NOT NULL DEFAULT 5432")
            conn.commit()
            print("Migration completed successfully")
        
        conn.close()
    except Exception as e:
        print(f"Error during database migration: {e}")

def get_user(username):
    """Get user from SQLite database by username"""
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("SELECT id, username, email, password_hash FROM users WHERE username = ?", (username,))
    user = cur.fetchone()
    conn.close()
    return user

def create_user(username, email, password):
    """Create a new user in SQLite database"""
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        password_hash = generate_password_hash(password)
        cur.execute(
            "INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
            (username, email, password_hash)
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        return False
    except Exception as e:
        print(f"Error creating user: {str(e)}")
        return False

def get_active_connection(user_id):
    """Get active database connection for the user"""
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT host, port, database, user_name, password 
        FROM db_connections 
        WHERE user_id = ? AND is_active = 1
    """, (user_id,))
    connection = cur.fetchone()
    conn.close()
    
    if connection:
        return {
            'host': connection[0],
            'port': connection[1],
            'database': connection[2],
            'user': connection[3],
            'password': connection[4]
        }
    return None

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        # Always use active connection from user's connections
        if 'user_id' in session:
            active_conn = get_active_connection(session.get('user_id'))
            if active_conn:
                print(f"Connecting to database: {active_conn['host']}, database: {active_conn['database']}, user: {active_conn['user']}")
                conn = psycopg2.connect(
                    host=active_conn['host'],
                    port=active_conn['port'],  # Use port from active connection
                    database=active_conn['database'],
                    user=active_conn['user'],
                    password=active_conn['password'],
                    # Add connection timeout and keepalives
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
                return conn
            else:
                print("No active connection found")
                return None
        print("No user_id in session")
        return None
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        return None

def get_auth_db_connection():
    """Get a connection to the authentication PostgreSQL database"""
    try:
        return psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
    except psycopg2.Error as e:
        print(f"Error connecting to auth database: {e}")
        return None

def init_db():
    """Initialize the authentication database tables"""
    try:
        conn = get_auth_db_connection()
        if not conn:
            print("Could not initialize authentication database")
            return
            
        cur = conn.cursor()
        
        # Create users table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(25) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        cur.close()
        conn.close()
        print("Authentication database initialized successfully!")
    except Exception as e:
        print(f"Error initializing authentication database: {e}")

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please login first', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def main():
    # Always redirect to login if not authenticated
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return redirect(url_for('index'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    # If already logged in, redirect to index
    if 'user_id' in session:
        return redirect(url_for('index'))
    
    form = LoginForm()
    if form.validate_on_submit():
        try:
            # Connect to Neon PostgreSQL for authentication
            conn = get_auth_db_connection()
            if not conn:
                flash('Authentication service unavailable', 'error')
                return render_template('login.html', form=form)
            
            cur = conn.cursor()
            
            # Check if user exists
            cur.execute(
                "SELECT id, username, password_hash FROM users WHERE username = %s",
                (form.username.data,)
            )
            user = cur.fetchone()
            
            if user and check_password_hash(user[2], form.password.data):
                session['user_id'] = user[0]
                session['username'] = user[1]
                flash('Login successful!', 'success')
                return redirect(url_for('index'))
            else:
                flash('Invalid username or password', 'error')
            
            cur.close()
            conn.close()
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
    
    return render_template('login.html', form=form)

@app.route('/index')
@login_required
def index():
    # Get user information
    user_id = session.get('user_id')
    username = session.get('username')
    
    # Initialize forms
    backup_form = BackupForm()
    
    try:
        # Get available databases
        databases = get_available_databases()
        backup_form.db_name.choices = [(db, db) for db in databases]
    except Exception as e:
        flash(f'Error fetching databases: {str(e)}', 'error')
        backup_form.db_name.choices = []
    
    return render_template('index.html', 
                         username=username,
                         form=backup_form,
                         databases=databases if 'databases' in locals() else [])

@app.route('/register', methods=['GET', 'POST'])
def register():
    # If already logged in, redirect to index
    if 'user_id' in session:
        return redirect(url_for('index'))
    
    form = RegistrationForm()
    if form.validate_on_submit():
        try:
            # Connect to Neon PostgreSQL for authentication
            conn = get_auth_db_connection()
            if not conn:
                flash('Registration service unavailable', 'error')
                return render_template('register.html', form=form)
            
            cur = conn.cursor()
            
            # Check if username exists
            cur.execute("SELECT 1 FROM users WHERE username = %s", (form.username.data,))
            if cur.fetchone():
                flash('Username already exists', 'error')
                return render_template('register.html', form=form)
            
            # Check if email exists
            cur.execute("SELECT 1 FROM users WHERE email = %s", (form.email.data,))
            if cur.fetchone():
                flash('Email already registered', 'error')
                return render_template('register.html', form=form)
            
            # Create new user
            password_hash = generate_password_hash(form.password.data)
            cur.execute(
                """
                INSERT INTO users (username, email, password_hash, created_at) 
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
                """,
                (form.username.data, form.email.data, password_hash)
            )
            user_id = cur.fetchone()[0]
            conn.commit()
            
            # Automatically log in the user after registration
            session['user_id'] = user_id
            session['username'] = form.username.data
            
            flash('Registration successful! Welcome!', 'success')
            return redirect(url_for('index'))
            
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
    
    return render_template('register.html', form=form)

@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out', 'success')
    return redirect(url_for('login'))

@app.route('/setup_database', methods=['GET', 'POST'])
def setup_database():
    if not session.get('is_admin'):
        flash('Only admin can access database setup', 'error')
        return redirect(url_for('login'))
        
    class DBSetupForm(FlaskForm):
        db_host = StringField('Database Host', [validators.DataRequired()], default='localhost')
        db_name = StringField('Database Name', [validators.DataRequired()], default='postgres')
        db_user = StringField('Database User', [validators.DataRequired()], default='postgres')
        db_password = PasswordField('Database Password', [validators.DataRequired()])
        submit = SubmitField('Save Database Configuration')
    
    form = DBSetupForm()
    if form.validate_on_submit():
        # Update database connection parameters
        global DB_CONFIG
        DB_CONFIG = {
            'host': form.db_host.data,
            'port': 5432,
            'database': form.db_name.data,
            'user': form.db_user.data,
            'password': form.db_password.data
        }
        
        try:
            # Test connection and create user table
            conn = get_db_connection()
            conn.autocommit = True
            cur = conn.cursor()
            
            # Create users table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(25) UNIQUE NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create admin user from session
            cur.execute(
                "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)",
                (session['admin_username'], session['admin_email'], session['admin_password'])
            )
            
            cur.close()
            conn.close()
            
            # Clear sensitive session data
            session.pop('admin_password', None)
            session.pop('admin_username', None)
            
            flash('Database configured successfully!', 'success')
            return redirect(url_for('login'))
            
        except Exception as e:
            flash(f'Database configuration failed: {str(e)}', 'error')
            
    return render_template('setup_database.html', form=form)

def get_available_databases():
    """Get list of available databases from PostgreSQL"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT datname 
            FROM pg_database 
            WHERE datistemplate = false 
            ORDER BY datname
        """)
        databases = [db[0] for db in cur.fetchall()]
        cur.close()
        conn.close()
        return databases
    except Exception as e:
        print(f"Error getting databases: {e}")
        return []

def get_ssh_client():
    """Create and return an SSH client connected to the database server"""
    if not USE_SSH:
        return None
        
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=SSH_PARAMS['hostname'],
            username=SSH_PARAMS['username'],
            password=SSH_PARAMS['password']
        )
        return ssh
    except Exception as e:
        print(f"SSH connection error: {str(e)}")
        return None

def create_remote_directory(path):
    """Create a directory on the remote server"""
    try:
        ssh = get_ssh_client()
        ssh.exec_command(f'mkdir -p {path}')
        ssh.close()
    except Exception as e:
        raise Exception(f"Failed to create remote directory: {str(e)}")

@app.route('/test_remote_connection', methods=['POST'])
@login_required
def test_remote_connection():
    try:
        data = request.get_json()
        host = data.get('host')
        port = int(data.get('port', 22))
        username = data.get('username')
        password = data.get('password')

        if not all([host, username, password]):
            return jsonify({
                'status': 'error',
                'message': 'Missing required connection parameters'
            })

        # Try to establish SSH connection
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            ssh.connect(
                hostname=host,
                port=port,
                username=username,
                password=password,
                timeout=10
            )
            ssh.close()
            
            # Store connection in session for future use
            session['remote_connection'] = {
                'host': host,
                'port': port,
                'username': username,
                'password': password
            }
            
            return jsonify({
                'status': 'success',
                'message': 'Successfully connected to remote server'
            })
        except Exception as e:
            return jsonify({
                'status': 'error',
                'message': f'Connection failed: {str(e)}'
            })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error: {str(e)}'
        })

@app.route('/list_remote_folders')
@login_required
def list_remote_folders():
    try:
        requested_path = request.args.get('path', None)
        server_type = request.args.get('server_type', 'local')
        
        if server_type == 'db':
            # Get connection details from request or session
            connection = session.get('remote_connection', None)
            if not connection:
                connection = {
                    'host': request.args.get('host'),
                    'port': int(request.args.get('port', 22)),
                    'username': request.args.get('username'),
                    'password': request.args.get('password')
                }
                
            if not all([connection['host'], connection['username'], connection['password']]):
                raise Exception("Remote server connection details not found")
            
            # Connect to remote server
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            try:
                ssh.connect(
                    hostname=connection['host'],
                    port=connection['port'],
                    username=connection['username'],
                    password=connection['password']
                )
                
                if not requested_path:
                    # List root directories
                    stdin, stdout, stderr = ssh.exec_command('df -h --output=target,avail,size')
                    filesystems = stdout.read().decode().strip().split('\n')[1:]  # Skip header
                    directories = []
                    
                    # Add root directory
                    directories.append({
                        'path': '/',
                        'name': '/',
                        'available': 'N/A',
                        'total': 'N/A'
                    })
                    
                    # Add mounted filesystems
                    for fs in filesystems:
                        parts = fs.split()
                        if len(parts) >= 3:
                            mount_point = parts[0]
                            available = parts[1]
                            total = parts[2]
                            directories.append({
                                'path': mount_point,
                                'name': mount_point,
                                'available': available,
                                'total': total
                            })
                else:
                    # List directories in the requested path
                    stdin, stdout, stderr = ssh.exec_command(f'ls -la "{requested_path}"')
                    items = stdout.read().decode().strip().split('\n')[1:]  # Skip total line
                    directories = []
                    
                    for item in items:
                        parts = item.split(None, 8)
                        if len(parts) >= 9:
                            name = parts[8]
                            if name not in ['.', '..'] and parts[0].startswith('d'):
                                full_path = os.path.join(requested_path, name)
                                
                                # Get space information
                                space_stdin, space_stdout, space_stderr = ssh.exec_command(
                                    f'df -h "{full_path}" --output=avail,size 2>/dev/null || echo "N/A N/A"'
                                )
                                space_info = space_stdout.read().decode().strip().split('\n')[-1].split()
                                available = space_info[0] if len(space_info) > 0 else 'N/A'
                                total = space_info[1] if len(space_info) > 1 else 'N/A'
                                
                                directories.append({
                                    'path': full_path,
                                    'name': name,
                                    'available': available,
                                    'total': total
                                })
                
                ssh.close()
                return jsonify({
                    'status': 'success',
                    'current_path': requested_path or '/',
                    'directories': directories
                })
            except Exception as e:
                if ssh:
                    ssh.close()
                raise Exception(f"Remote server error: {str(e)}")
        else:
            # Local filesystem handling
            if not requested_path:
                # On Windows, list all drives
                if os.name == 'nt':
                    import win32api
                    drives = win32api.GetLogicalDriveStrings().split('\000')[:-1]
                    directories = []
                    for drive in drives:
                        try:
                            total, used, free = shutil.disk_usage(drive)
                            directories.append({
                                'path': drive,
                                'name': drive,
                                'available': f"{free // (2**30)} GB",
                                'total': f"{total // (2**30)} GB"
                            })
                        except:
                            continue
                else:
                    # On Unix/Linux, list root directory
                    requested_path = '/'
            
            if requested_path:
                directories = []
                try:
                    for item in os.listdir(requested_path):
                        full_path = os.path.join(requested_path, item)
                        if os.path.isdir(full_path):
                            try:
                                total, used, free = shutil.disk_usage(full_path)
                                directories.append({
                                    'path': full_path,
                                    'name': item,
                                    'available': f"{free // (2**30)} GB",
                                    'total': f"{total // (2**30)} GB"
                                })
                            except:
                                directories.append({
                                    'path': full_path,
                                    'name': item,
                                    'available': 'N/A',
                                    'total': 'N/A'
                                })
                except PermissionError:
                    return jsonify({
                        'status': 'error',
                        'message': 'Permission denied for this directory'
                    })
            
            return jsonify({
                'status': 'success',
                'current_path': requested_path or '',
                'directories': directories
            })
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/create_folder', methods=['POST'])
@login_required
def create_folder():
    try:
        data = request.get_json()
        parent_path = data.get('path', '')
        folder_name = data.get('folder_name', '').strip()
        server_type = data.get('server_type', 'local')
        
        if not folder_name:
            return jsonify({'status': 'error', 'message': 'Folder name is required'})
            
        # Validate folder name (no special characters or paths)
        if '/' in folder_name or '\\' in folder_name:
            return jsonify({'status': 'error', 'message': 'Invalid folder name. Do not include path separators.'})
            
        new_folder_path = os.path.join(parent_path, folder_name)
        
        # Get active database connection to verify access
        if server_type == 'db':
            db_params = get_active_connection(session['user_id'])
            if not db_params:
                raise Exception("No active database connection. Please select and test a connection first.")
        
        if server_type == 'db':
            ssh = get_ssh_client()
            if not ssh:
                raise Exception("Could not connect to database server")
            
            # Check if parent directory exists and is writable
            stdin, stdout, stderr = ssh.exec_command(f'test -d "{parent_path}" && test -w "{parent_path}" && echo "yes" || echo "no"')
            can_write = stdout.read().decode().strip() == "yes"
            
            if not can_write:
                raise Exception("Permission denied: Cannot write to this location")
            
            # Check if folder already exists
            stdin, stdout, stderr = ssh.exec_command(f'test -e "{new_folder_path}" && echo "exists"')
            if stdout.read().decode().strip() == "exists":
                raise Exception("A file or directory with this name already exists")
            
            # Create the directory
            stdin, stdout, stderr = ssh.exec_command(f'mkdir -p "{new_folder_path}"')
            error = stderr.read().decode().strip()
            ssh.close()
            
            if error:
                raise Exception(error)
        else:
            # Local filesystem handling
            if not os.path.exists(parent_path):
                raise Exception("Parent directory does not exist")
                
            if not os.access(parent_path, os.W_OK):
                raise Exception("Permission denied: Cannot write to this location")
                
            if os.path.exists(new_folder_path):
                raise Exception("A file or directory with this name already exists")
                
            os.makedirs(new_folder_path)
            
        return jsonify({
            'status': 'success',
            'message': 'Folder created successfully',
            'path': new_folder_path
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

@app.route('/list_backups')
@login_required
def list_backups():
    try:
        ssh = get_ssh_client()
        backup_dir = SSH_PARAMS['default_backup_path']
        stdin, stdout, stderr = ssh.exec_command(f'ls -l {backup_dir}/*.backup')
        files = stdout.read().decode().splitlines()
        
        backups = []
        for file in files[1:]:  # Skip the first line (total)
            parts = file.split()
            if len(parts) >= 8:
                size = int(parts[4])
                date = f"{parts[5]} {parts[6]} {parts[7]}"
                name = parts[-1]
                backups.append({
                    'name': os.path.basename(name),
                    'size': f'{size / (1024*1024):.2f} MB',
                    'date': date
                })
        
        ssh.close()
        return render_template('list_backups.html', backups=backups)
    except Exception as e:
        flash(f'Error listing backups: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/check_connection')
@login_required
def check_connection():
    try:
        # Get active connection parameters
        db_params = get_active_connection(session['user_id'])
        if not db_params:
            return jsonify({
                'status': 'error',
                'message': 'No active connection. Please select and activate a database connection from the Connections menu.'
            })
        
        # Try to connect with the active connection parameters
        conn = psycopg2.connect(
            host=db_params['host'],
            port=5432,  # Default PostgreSQL port
            database=db_params['database'],
            user=db_params['user'],
            password=db_params['password']
        )
        
        # Test if connection is working by executing a simple query
        cur = conn.cursor()
        cur.execute('SELECT version()')
        version = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'message': f'Successfully connected to PostgreSQL server\nServer version: {version}'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Connection failed: {str(e)}'
        })

@app.route('/connections', methods=['GET'])
@login_required
def list_connections():
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, host, database, user_name, is_active 
        FROM db_connections 
        WHERE user_id = ? 
        ORDER BY name
    """, (session['user_id'],))
    connections = cur.fetchall()
    conn.close()
    return render_template('connections.html', connections=connections)

@app.route('/connections/add', methods=['GET', 'POST'])
@login_required
def add_connection():
    form = ConnectionForm()
    if form.validate_on_submit():
        if form.test_connection.data:
            # Test the connection
            try:
                test_conn = psycopg2.connect(
                    host=form.host.data,
                    port=form.port.data,  # Use the port from the form
                    database=form.database.data,
                    user=form.user_name.data,
                    password=form.password.data,
                    connect_timeout=10
                )
                test_conn.close()
                flash('Connection test successful!', 'success')
            except Exception as e:
                flash(f'Connection test failed: {str(e)}', 'error')
            return render_template('add_connection.html', form=form)
        
        try:
            conn = sqlite3.connect(SQLITE_DB)
            cur = conn.cursor()
            
            # Check if connection name already exists for this user
            cur.execute("""
                SELECT 1 FROM db_connections 
                WHERE user_id = ? AND name = ?
            """, (session['user_id'], form.name.data))
            
            if cur.fetchone():
                flash('Connection name already exists', 'error')
                return render_template('add_connection.html', form=form)
            
            # Test the connection before saving
            try:
                test_conn = psycopg2.connect(
                    host=form.host.data,
                    port=form.port.data,  # Use the port from the form
                    database=form.database.data,
                    user=form.user_name.data,
                    password=form.password.data,
                    connect_timeout=10
                )
                test_conn.close()
            except Exception as e:
                flash(f'Connection test failed: {str(e)}', 'error')
                return render_template('add_connection.html', form=form)
            
            # Save the connection
            cur.execute(
                """INSERT INTO db_connections 
                   (user_id, name, host, port, database, user_name, password) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (session['user_id'], form.name.data, form.host.data, 
                 form.port.data, form.database.data, form.user_name.data, form.password.data)
            )
            conn.commit()
            
            # Set as active connection if it's the only one
            cur.execute("SELECT COUNT(*) FROM db_connections WHERE user_id = ?", (session['user_id'],))
            if cur.fetchone()[0] == 1:
                cur.execute("""
                    UPDATE db_connections 
                    SET is_active = 1 
                    WHERE user_id = ? AND name = ?
                """, (session['user_id'], form.name.data))
                conn.commit()
            
            conn.close()
            flash('Connection added successfully!', 'success')
            return redirect(url_for('list_connections'))
            
        except sqlite3.IntegrityError:
            flash('Connection name already exists', 'error')
        except Exception as e:
            flash(f'Error adding connection: {str(e)}', 'error')
        finally:
            if 'conn' in locals():
                conn.close()
    
    return render_template('add_connection.html', form=form)

@app.route('/connections/activate/<int:conn_id>')
@login_required
def activate_connection(conn_id):
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        
        # Verify connection belongs to user
        cur.execute("""
            SELECT host, database, user_name, password 
            FROM db_connections 
            WHERE id = ? AND user_id = ?
        """, (conn_id, session['user_id']))
        connection = cur.fetchone()
        
        if connection:
            # Test connection before activating
            test_params = {
                'host': connection[0],
                'database': connection[1],
                'user': connection[2],
                'password': connection[3]
            }
            try:
                test_conn = get_db_connection()
                test_conn.close()
                
                # Deactivate all connections
                cur.execute("""
                    UPDATE db_connections 
                    SET is_active = 0 
                    WHERE user_id = ?
                """, (session['user_id'],))
                
                # Activate selected connection
                cur.execute("""
                    UPDATE db_connections 
                    SET is_active = 1 
                    WHERE id = ?
                """, (conn_id,))
                
                conn.commit()
                flash('Connection activated successfully!', 'success')
            except Exception as e:
                flash(f'Connection test failed: {str(e)}', 'error')
        else:
            flash('Connection not found!', 'error')
            
        conn.close()
    except Exception as e:
        flash(f'Error activating connection: {str(e)}', 'error')
    
    return redirect(url_for('list_connections'))

@app.route('/connections/delete/<int:conn_id>')
@login_required
def delete_connection(conn_id):
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM db_connections 
            WHERE id = ? AND user_id = ?
        """, (conn_id, session['user_id']))
        conn.commit()
        conn.close()
        flash('Connection deleted successfully!', 'success')
    except Exception as e:
        flash(f'Error deleting connection: {str(e)}', 'error')
    
    return redirect(url_for('list_connections'))

@app.route('/monitoring', methods=['GET', 'POST'])
@login_required
def monitoring():
    conn_form = ConnectionSelectorForm()
    connections = get_user_connections(session['user_id'])
    conn_form.connection.choices = [(str(c[0]), c[1]) for c in connections]
    
    if conn_form.validate_on_submit() and conn_form.test.data:
        conn_id = int(conn_form.connection.data)
        db_params = get_connection_by_id(session['user_id'], conn_id)
        
        try:
            conn = get_db_connection()
            
            # Get active connections
            cur = conn.cursor()
            cur.execute("""
                SELECT datname, usename, client_addr, backend_start, state
                FROM pg_stat_activity
                WHERE datname IS NOT NULL
            """)
            connections_data = cur.fetchall()
            
            # Get database sizes
            cur.execute("""
                SELECT datname, pg_size_pretty(pg_database_size(datname)) as size
                FROM pg_database
                WHERE datistemplate = false
            """)
            db_sizes = cur.fetchall()
            
            # Get table statistics
            cur.execute("""
                SELECT schemaname, relname, n_live_tup, n_dead_tup, 
                       pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as total_size
                FROM pg_stat_user_tables
                ORDER BY n_live_tup DESC
                LIMIT 10
            """)
            table_stats = cur.fetchall()
            
            cur.close()
            conn.close()
            
            session['selected_connection'] = conn_id
            flash('Connection successful!', 'success')
            
            return render_template('monitoring.html',
                                 conn_form=conn_form,
                                 connections=connections_data,
                                 db_sizes=db_sizes,
                                 table_stats=table_stats,
                                 show_data=True)
        except Exception as e:
            flash(f'Connection failed: {str(e)}', 'error')
    
    return render_template('monitoring.html', conn_form=conn_form, show_data=False)

def execute_backup_command(cmd, env=None):
    """Execute backup command with proper error handling"""
    try:
        print(f"Executing command: {cmd}")
        
        # For Windows, we need to use shell=True to handle environment variables
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env or os.environ
        )
        output, error = process.communicate()
        output = output.decode().strip() if output else ""
        error = error.decode().strip() if error else ""
        
        print(f"Command output: {output}")
        print(f"Command error: {error}")
        
        if process.returncode != 0:
            # Check for specific error codes on Windows
            if os.name == 'nt' and process.returncode in [3221225781, 3221225595]:
                # These are Windows-specific error codes that often indicate missing DLLs
                # or other issues with the executable
                raise Exception(
                    f"The pg_dump executable failed to run. This might be due to missing dependencies. "
                    f"Try installing PostgreSQL client tools directly on your system. "
                    f"Error code: {process.returncode}"
                )
            else:
                raise Exception(f"Command failed with return code {process.returncode}. Error: {error}")
        
        return output, error
    except Exception as e:
        print(f"Error executing command: {e}")
        raise

def construct_backup_command(backup_method, backup_type, file_type, backup_path, db_name, db_params):
    """Construct backup command based on parameters"""
    try:
        # Check if the backup path is a remote path (starts with /)
        is_remote_path = backup_path.startswith('/')
        
        # If it's a remote path, create a local temporary path
        local_backup_path = backup_path
        if is_remote_path:
            # Create a temp directory if it doesn't exist
            temp_dir = os.path.join(os.getcwd(), 'temp_backups')
            os.makedirs(temp_dir, exist_ok=True)
            
            # Use the same filename but in the local temp directory
            filename = os.path.basename(backup_path)
            local_backup_path = os.path.join(temp_dir, filename).replace('\\', '/')
            print(f"Using local temporary path: {local_backup_path}")
        
        if backup_method == 'pg_dump':
            if os.name == 'nt':  # Windows
                pg_dump_exe = ensure_pg_dump_exists()
                if not pg_dump_exe:
                    raise Exception(
                        "Could not find or download pg_dump. Please either:\n"
                        "1. Install PostgreSQL 14 from https://www.enterprisedb.com/downloads/postgres-postgresql-downloads\n"
                        "2. Set POSTGRES_BIN_PATH environment variable to point to your PostgreSQL bin directory\n"
                        "3. Check your internet connection and try again to let the app download pg_dump"
                    )
                print(f"Using pg_dump from: {pg_dump_exe}")
                
                # For Windows, use a batch-style command
                cmd = [
                    f'set PGPASSWORD={db_params["password"]} && ',
                    f'"{pg_dump_exe}" ',
                    f'-h {db_params["host"]} ',
                    f'-p {db_params.get("port", 5432)} ',
                    f'-U {db_params["user"]} ',
                    f'-d {db_name} '  # Explicitly specify database name
                ]
            else:  # Unix/Linux
                cmd = [
                    f'PGPASSWORD="{db_params["password"]}" pg_dump ',
                    f'-h {db_params["host"]} ',
                    f'-p {db_params.get("port", 5432)} ',
                    f'-U {db_params["user"]} ',
                    f'-d {db_name} '  # Explicitly specify database name
                ]

            # Add backup type options
            if backup_type == 'schema':
                cmd.append('--schema-only ')
            elif backup_type == 'data':
                cmd.append('--data-only ')

            # Add file format options
            if file_type == 'custom':
                cmd.append('-F c ')  # Custom format
            elif file_type == 'tar':
                cmd.append('-F t ')  # Tar format
            elif file_type == 'directory':
                cmd.append('-F d ')  # Directory format
            else:
                cmd.append('-F p ')  # Plain text SQL

            # Add output file option
            cmd.append(f'-f "{local_backup_path}" ')

            # Join command parts
            command = ''.join(cmd)
            
            # If it's a remote path, we'll need to transfer the file after backup
            if is_remote_path:
                # Store the original remote path and local temp path for later transfer
                command = command + f" && echo REMOTE_PATH:{backup_path} && echo LOCAL_PATH:{local_backup_path}"
            
            print(f"Constructed command: {command}")
            return command

    except Exception as e:
        print(f"Error constructing backup command: {e}")
        raise

@app.route('/backup_restore', methods=['GET', 'POST'])
@login_required
def backup_restore():
    conn_form = ConnectionSelectorForm()
    connections = get_user_connections(session['user_id'])
    conn_form.connection.choices = [(str(c[0]), c[1]) for c in connections]
    
    backup_form = BackupForm()
    
    # Initialize db_params
    db_params = get_active_connection(session['user_id'])
    show_backup_form = bool(db_params)
    
    print(f"Request method: {request.method}")
    if request.method == 'POST':
        print(f"Form data: {request.form}")
        print(f"Submit button pressed: {request.form.get('submit', False)}")
    
    # Handle connection test
    if request.method == 'POST' and conn_form.validate_on_submit() and conn_form.test.data:
        try:
            conn_id = int(conn_form.connection.data)
            db_params = get_connection_by_id(session['user_id'], conn_id)
            
            if not db_params:
                flash('Connection not found', 'error')
                return render_template('backup_restore.html',
                                    conn_form=conn_form,
                                    backup_form=backup_form,
                                    show_backup_form=False)
            
            # Test connection and get databases
            try:
                conn = psycopg2.connect(
                    host=db_params['host'],
                    port=5432,
                    database=db_params['database'],
                    user=db_params['user'],
                    password=db_params['password'],
                    connect_timeout=10
                )
                cur = conn.cursor()
                
                # Get available databases
                cur.execute("""
                    SELECT datname 
                    FROM pg_database 
                    WHERE datistemplate = false 
                    ORDER BY datname
                """)
                databases = [db[0] for db in cur.fetchall()]
                backup_form.db_name.choices = [(db, db) for db in databases]
                
                cur.close()
                conn.close()
                
                # Update active connection
                conn = sqlite3.connect(SQLITE_DB)
                cur = conn.cursor()
                
                # Deactivate all connections
                cur.execute("""
                    UPDATE db_connections 
                    SET is_active = 0 
                    WHERE user_id = ?
                """, (session['user_id'],))
                
                # Activate selected connection
                cur.execute("""
                    UPDATE db_connections 
                    SET is_active = 1 
                    WHERE id = ?
                """, (conn_id,))
                
                conn.commit()
                conn.close()
                
                flash('Connection test successful!', 'success')
                show_backup_form = True
            except Exception as e:
                flash(f'Connection test failed: {str(e)}', 'error')
                show_backup_form = False
                
        except Exception as e:
            flash(f'Error during connection test: {str(e)}', 'error')
            show_backup_form = False
            
        return render_template('backup_restore.html',
                            conn_form=conn_form,
                            backup_form=backup_form,
                            show_backup_form=show_backup_form)
    
    # Handle backup form submission
    elif request.method == 'POST' and request.form.get('submit') == 'true':
        print("Processing backup form submission with submit=true")
        try:
            db_params = get_active_connection(session['user_id'])
            if not db_params:
                flash('Please select and test a database connection first', 'error')
                return render_template('backup_restore.html',
                                    conn_form=conn_form,
                                    backup_form=backup_form,
                                    show_backup_form=False)

            backup_method = request.form.get('backup_method')
            backup_type = request.form.get('backup_type')
            backup_location = request.form.get('backup_location', '').replace('\\', '/')
            db_name = request.form.get('db_name')
            file_type = request.form.get('file_type')
            
            print(f"Backup parameters: method={backup_method}, type={backup_type}, location={backup_location}, db={db_name}, file_type={file_type}")
            
            if not all([backup_method, backup_type, backup_location, db_name, file_type]):
                missing = []
                if not backup_method: missing.append('Backup Method')
                if not backup_type: missing.append('Backup Type')
                if not backup_location: missing.append('Backup Location')
                if not db_name: missing.append('Database Name')
                if not file_type: missing.append('File Type')
                flash(f'Please fill in all required fields: {", ".join(missing)}', 'error')
                return render_template('backup_restore.html',
                                    conn_form=conn_form,
                                    backup_form=backup_form,
                                    show_backup_form=True)

            # Generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_name = request.form.get('file_name') or f'{db_name}_{timestamp}'
            
            # Add file extension
            if file_type == 'custom':
                file_name += '.backup'
            elif file_type == 'sql':
                file_name += '.sql'
            elif file_type == 'tar':
                file_name += '.tar'
            elif file_type == 'directory':
                file_name = f'{file_name}_dir'
            
            # Check if it's a remote path (starts with /)
            is_remote_path = backup_location.startswith('/')
            
            # If it's a local path, create the directory
            if not is_remote_path:
                try:
                    print(f"Creating backup directory: {backup_location}")
                    os.makedirs(backup_location, exist_ok=True)
                except Exception as e:
                    flash(f'Failed to create backup directory: {str(e)}', 'error')
                    return render_template('backup_restore.html',
                                        conn_form=conn_form,
                                        backup_form=backup_form,
                                        show_backup_form=True)
            
            # Test directory permissions
            test_file = os.path.join(backup_location, 'test_write')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                print("Directory permissions test passed")
            except Exception as e:
                flash(f'Backup location is not writable: {str(e)}', 'error')
                return render_template('backup_restore.html',
                                    conn_form=conn_form,
                                    backup_form=backup_form,
                                    show_backup_form=True)
            else:
                # For remote paths, we'll create a temporary local directory
                # and transfer the file after backup
                print(f"Using remote backup location: {backup_location}")
                
                # Create temp directory for local backup
                temp_dir = os.path.join(os.getcwd(), 'temp_backups')
                os.makedirs(temp_dir, exist_ok=True)
            
            # Set backup path
            backup_path = os.path.join(backup_location, file_name).replace('\\', '/')
            print(f"Backup path: {backup_path}")
            
            try:
                # Construct backup command
                cmd = construct_backup_command(
                    backup_method=backup_method,
                    backup_type=backup_type,
                    file_type=file_type,
                    backup_path=backup_path,
                    db_name=db_name,
                    db_params=db_params
                )
                
                print(f"Executing backup command: {cmd}")
                
                # Set environment variables
                env = dict(os.environ)
                env['PGPASSWORD'] = db_params['password']
                
                try:
                    # Try to execute backup command using pg_dump
                    output, error = execute_backup_command(cmd, env)
                    
                    if error and not error.startswith(('pg_dump: warning:', 'pg_basebackup: warning:')):
                        raise Exception(error)
                except Exception as pg_dump_error:
                    # If pg_dump fails, try the fallback method
                    print(f"pg_dump failed: {str(pg_dump_error)}")
                    print("Trying fallback method with psycopg2...")
                    
                    # Determine the local path to use
                    local_path = backup_path
                    if is_remote_path:
                        # Use the temporary local path
                        temp_dir = os.path.join(os.getcwd(), 'temp_backups')
                        os.makedirs(temp_dir, exist_ok=True)
                        filename = os.path.basename(backup_path)
                        local_path = os.path.join(temp_dir, filename).replace('\\', '/')
                    
                    # Create backup using psycopg2
                    create_backup_with_psycopg2(
                        db_params=db_params,
                        db_name=db_name,
                        backup_path=local_path,
                        backup_type=backup_type
                    )
                    
                    # Set output for remote path handling
                    output = f"LOCAL_PATH:{local_path}\nREMOTE_PATH:{backup_path}"
                
                # Check if we need to transfer the file to a remote server
                if is_remote_path:
                    # Extract local and remote paths from the output
                    local_path = None
                    remote_path = None
                    
                    for line in output.split('\n'):
                        if line.startswith('LOCAL_PATH:'):
                            local_path = line.replace('LOCAL_PATH:', '').strip()
                        elif line.startswith('REMOTE_PATH:'):
                            remote_path = line.replace('REMOTE_PATH:', '').strip()
                    
                    if local_path and remote_path:
                        print(f"Transferring backup from {local_path} to {remote_path}")
                        
                        # Get remote connection details from session
                        remote_connection = session.get('remote_connection')
                        if not remote_connection:
                            flash(f'Remote connection details not found. Please select a remote folder again.', 'error')
                            return render_template('backup_restore.html',
                                                conn_form=conn_form,
                                                backup_form=backup_form,
                                                show_backup_form=True)
                        
                        try:
                            # Create SSH connection
                            ssh = paramiko.SSHClient()
                            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                            ssh.connect(
                                hostname=remote_connection['host'],
                                port=remote_connection['port'],
                                username=remote_connection['username'],
                                password=remote_connection['password'],
                                timeout=10
                            )
                            
                            # Create remote directory if it doesn't exist
                            remote_dir = os.path.dirname(remote_path)
                            ssh.exec_command(f'mkdir -p {remote_dir}')
                            
                            # Transfer the file using SFTP
                            sftp = ssh.open_sftp()
                            sftp.put(local_path, remote_path)
                            sftp.close()
                            
                            # Close SSH connection
                            ssh.close()
                            
                            print(f"Backup file transferred successfully to {remote_path}")
                            
                            # Delete the local temporary file
                            os.remove(local_path)
                            print(f"Temporary local file {local_path} deleted")
                            
                            flash(f'Backup created successfully at: {backup_path}', 'success')
                        except Exception as ssh_error:
                            print(f"SSH error: {str(ssh_error)}")
                            flash(f'Backup created locally at: {local_path}, but could not transfer to remote server: {str(ssh_error)}', 'warning')
                    else:
                        flash(f'Backup created successfully at: {local_path}', 'success')
                else:
                    flash(f'Backup created successfully at: {backup_path}', 'success')
                
            except Exception as e:
                flash(f'Backup failed: {str(e)}', 'error')
                print(f"Backup error: {str(e)}")
            
            return render_template('backup_restore.html',
                                conn_form=conn_form,
                                backup_form=backup_form,
                                show_backup_form=True)
                                
        except Exception as e:
            flash(f'Error during backup: {str(e)}', 'error')
            print(f"Error during backup process: {str(e)}")
            return render_template('backup_restore.html',
                                conn_form=conn_form,
                                backup_form=backup_form,
                                show_backup_form=True)
    
    # For GET requests or connection tests
    if db_params:
        try:
            # Get available databases for the active connection
            conn = psycopg2.connect(
                host=db_params['host'],
                port=5432,
                database=db_params['database'],
                user=db_params['user'],
                password=db_params['password'],
                connect_timeout=10
            )
            cur = conn.cursor()
            cur.execute("""
                SELECT datname 
                FROM pg_database 
                WHERE datistemplate = false 
                ORDER BY datname
            """)
            databases = [db[0] for db in cur.fetchall()]
            backup_form.db_name.choices = [(db, db) for db in databases]
            cur.close()
            conn.close()
        except Exception as e:
            flash(f'Error fetching databases: {str(e)}', 'error')
            backup_form.db_name.choices = []
    
    return render_template('backup_restore.html',
                         conn_form=conn_form,
                         backup_form=backup_form,
                         show_backup_form=show_backup_form)

@app.route('/performance', methods=['GET', 'POST'])
@login_required
def performance():
    conn_form = ConnectionSelectorForm()
    connections = get_user_connections(session['user_id'])
    conn_form.connection.choices = [(str(c[0]), c[1]) for c in connections]
    
    slow_queries = []
    index_stats = []
    io_stats = []
    extension_status = {
        'library_loaded': False,
        'extension_created': False
    }
    
    if conn_form.validate_on_submit() and conn_form.test.data:
        conn_id = int(conn_form.connection.data)
        db_params = get_connection_by_id(session['user_id'], conn_id)
        
        try:
            conn = get_db_connection()
            conn.autocommit = True  # Use autocommit to prevent transaction issues
            
            # Check if pg_stat_statements extension exists
            cur = conn.cursor()
            try:
                # First check if the extension is created in the current database
                cur.execute("""
                    SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
                """)
                extension_created = cur.fetchone() is not None
                extension_status['extension_created'] = extension_created
                
                # Then check if the library is loaded (configured in postgresql.conf)
                cur.execute("""
                    SELECT 1 FROM pg_settings 
                    WHERE name = 'shared_preload_libraries' 
                    AND setting LIKE '%pg_stat_statements%'
                """)
                library_loaded = cur.fetchone() is not None
                extension_status['library_loaded'] = library_loaded
                
                has_pg_stat_statements = extension_created and library_loaded
                
                # Get slow queries if extension exists
                if has_pg_stat_statements:
                    try:
                        # First check PostgreSQL version to determine which columns to use
                        cur.execute("SELECT current_setting('server_version_num')::integer")
                        pg_version = int(cur.fetchone()[0])
                        
                        # For PostgreSQL 13 and newer, use total_exec_time, for older versions use total_time
                        if pg_version >= 130000:  # PostgreSQL 13.0 or newer
                            cur.execute("""
                                SELECT query, calls, total_exec_time as total_time, total_exec_time/calls as mean_time, rows
                                FROM pg_stat_statements
                                ORDER BY total_exec_time DESC
                                LIMIT 10
                            """)
                        else:
                            cur.execute("""
                                SELECT query, calls, total_time, mean_time, rows
                                FROM pg_stat_statements
                                ORDER BY total_time DESC
                                LIMIT 10
                            """)
                        slow_queries = cur.fetchall()
                    except Exception as e:
                        flash(f"Could not query pg_stat_statements: {str(e)}", "warning")
                        slow_queries = []
                else:
                    if library_loaded and not extension_created:
                        flash("The pg_stat_statements library is loaded, but the extension is not created in this database. Use the button below to create it.", "warning")
                    elif not library_loaded:
                        flash("The pg_stat_statements extension is not enabled in postgresql.conf or the server has not been restarted. Some performance metrics will not be available.", "warning")
                    else:
                        flash("The pg_stat_statements extension is not properly configured. Some performance metrics will not be available.", "warning")
            except Exception as e:
                flash(f"Error checking pg_stat_statements: {str(e)}", "warning")
            finally:
                cur.close()
            
            # Get index usage statistics
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT schemaname, relname, indexrelname, 
                           idx_scan as index_scans,
                           idx_tup_read as tuples_read,
                           idx_tup_fetch as tuples_fetched
                    FROM pg_stat_user_indexes
                    ORDER BY idx_scan DESC
                    LIMIT 10
                """)
                index_stats = cur.fetchall()
            except Exception as e:
                flash(f"Could not query index statistics: {str(e)}", "warning")
                index_stats = []
            finally:
                cur.close()
            
            # Get table I/O statistics
            cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT schemaname, relname,
                           heap_blks_read, heap_blks_hit,
                           idx_blks_read, idx_blks_hit
                    FROM pg_statio_user_tables
                    ORDER BY heap_blks_read + idx_blks_read DESC
                    LIMIT 10
                """)
                io_stats = cur.fetchall()
            except Exception as e:
                flash(f"Could not query table I/O statistics: {str(e)}", "warning")
                io_stats = []
            finally:
                cur.close()
            
            conn.close()
            
            session['selected_connection'] = conn_id
            flash('Connection successful!', 'success')
            
            return render_template('performance.html',
                                 conn_form=conn_form,
                                 slow_queries=slow_queries,
                                 index_stats=index_stats,
                                 io_stats=io_stats,
                                 extension_status=extension_status,
                                 show_data=True)
        except Exception as e:
            flash(f'Connection failed: {str(e)}', 'error')
    
    return render_template('performance.html', conn_form=conn_form, extension_status=extension_status, show_data=False)

def get_user_connections(user_id):
    """Get all connections for a user"""
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, host, database, user_name, password, is_active
        FROM db_connections 
        WHERE user_id = ? 
        ORDER BY name
    """, (user_id,))
    connections = cur.fetchall()
    conn.close()
    return connections

def get_connection_by_id(user_id, conn_id):
    """Get connection details by ID"""
    conn = sqlite3.connect(SQLITE_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT host, port, database, user_name, password
        FROM db_connections 
        WHERE user_id = ? AND id = ?
    """, (user_id, conn_id))
    connection = cur.fetchone()
    conn.close()
    
    if connection:
        return {
            'host': connection[0],
            'port': connection[1],
            'database': connection[2],
            'user': connection[3],
            'password': connection[4]
        }
    return None

def get_postgres_bin_path():
    """Get PostgreSQL bin directory path"""
    try:
        # First check if manually configured
        if POSTGRES_BIN_PATH and os.path.exists(POSTGRES_BIN_PATH):
            print(f"Using configured POSTGRES_BIN_PATH: {POSTGRES_BIN_PATH}")
            return POSTGRES_BIN_PATH

        # Check specific version path first (PostgreSQL 14)
        specific_paths = [
            r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
            r"C:\Program Files (x86)\PostgreSQL\14\bin\pg_dump.exe",
            r"C:\PostgreSQL\14\bin\pg_dump.exe"
        ]
        
        for path in specific_paths:
            if os.path.exists(path):
                print(f"Found pg_dump in specific path: {path}")
                return path

        # Check if we have a local copy in the app directory
        app_dir = os.path.dirname(os.path.abspath(__file__))
        pg_dump_dir = os.path.join(app_dir, 'pg_dump')
        pg_dump_exe = os.path.join(pg_dump_dir, 'pg_dump.exe')
        
        if os.path.exists(pg_dump_exe):
            print(f"Using existing pg_dump: {pg_dump_exe}")
            return pg_dump_exe

        # Download portable version
        print("Downloading portable pg_dump...")
        os.makedirs(pg_dump_dir, exist_ok=True)
        
        # URL for PostgreSQL 14 Windows binaries
        url = "https://get.enterprisedb.com/postgresql/postgresql-14.10-1-windows-x64-binaries.zip"
        zip_path = os.path.join(pg_dump_dir, "pgsql.zip")
        
        try:
            # Download the zip file
            print(f"Downloading from {url}")
            urllib.request.urlretrieve(url, zip_path)
            
            # Extract required files
            print("Extracting files...")
            with zipfile.ZipFile(zip_path) as z:
                for file in z.namelist():
                    if file.endswith(('pg_dump.exe', 'libpq.dll', 'libintl-8.dll', 'libiconv-2.dll', 'libwinpthread-1.dll')):
                        z.extract(file, pg_dump_dir)
                        extracted_file = os.path.join(pg_dump_dir, file)
                        final_file = os.path.join(pg_dump_dir, os.path.basename(file))
                        if extracted_file != final_file:
                            if os.path.exists(final_file):
                                os.remove(final_file)
                            os.rename(extracted_file, final_file)
            
            # Clean up
            os.remove(zip_path)
            
            if os.path.exists(pg_dump_exe):
                print(f"Successfully downloaded pg_dump to: {pg_dump_exe}")
                return pg_dump_exe
            
            raise Exception("pg_dump.exe not found after extraction")
        except Exception as e:
            print(f"Error during download/extraction: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            raise
        
    except Exception as e:
        print(f"Error ensuring pg_dump exists: {e}")
        return None

def ensure_pg_dump_exists():
    """Ensure pg_dump exists, download portable version if needed"""
    try:
        # First check if manually configured through environment variable
        if POSTGRES_BIN_PATH:
            pg_dump_path = os.path.join(POSTGRES_BIN_PATH, 'pg_dump.exe')
            if os.path.exists(pg_dump_path):
                print(f"Using pg_dump from environment variable: {pg_dump_path}")
                return pg_dump_path

        # Check specific version path first (PostgreSQL 14)
        specific_paths = [
            r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe",
            r"C:\Program Files (x86)\PostgreSQL\14\bin\pg_dump.exe",
            r"C:\PostgreSQL\14\bin\pg_dump.exe"
        ]
        
        for path in specific_paths:
            if os.path.exists(path):
                print(f"Found pg_dump in specific path: {path}")
                return path

        # Check if we have a local copy in the app directory
        app_dir = os.path.dirname(os.path.abspath(__file__))
        pg_dump_dir = os.path.join(app_dir, 'pg_dump')
        pg_dump_exe = os.path.join(pg_dump_dir, 'pg_dump.exe')
        
        if os.path.exists(pg_dump_exe):
            print(f"Using existing pg_dump: {pg_dump_exe}")
            return pg_dump_exe

        # Download portable version
        print("Downloading portable pg_dump...")
        os.makedirs(pg_dump_dir, exist_ok=True)
        
        # URL for PostgreSQL 14 Windows binaries
        url = "https://get.enterprisedb.com/postgresql/postgresql-14.10-1-windows-x64-binaries.zip"
        zip_path = os.path.join(pg_dump_dir, "pgsql.zip")
        
        try:
            # Download the zip file
            print(f"Downloading from {url}")
            urllib.request.urlretrieve(url, zip_path)
            
            # Extract required files
            print("Extracting files...")
            with zipfile.ZipFile(zip_path) as z:
                for file in z.namelist():
                    if file.endswith(('pg_dump.exe', 'libpq.dll', 'libintl-8.dll', 'libiconv-2.dll', 'libwinpthread-1.dll')):
                        z.extract(file, pg_dump_dir)
                        extracted_file = os.path.join(pg_dump_dir, file)
                        final_file = os.path.join(pg_dump_dir, os.path.basename(file))
                        if extracted_file != final_file:
                            if os.path.exists(final_file):
                                os.remove(final_file)
                            os.rename(extracted_file, final_file)
            
            # Clean up
            os.remove(zip_path)
            
            if os.path.exists(pg_dump_exe):
                print(f"Successfully downloaded pg_dump to: {pg_dump_exe}")
                return pg_dump_exe
            
            raise Exception("pg_dump.exe not found after extraction")
        except Exception as e:
            print(f"Error during download/extraction: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            raise
        
    except Exception as e:
        print(f"Error ensuring pg_dump exists: {e}")
        return None

@app.route('/get_available_databases')
@login_required
def get_available_databases_ajax():
    """Get list of available databases from PostgreSQL via AJAX"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'status': 'error',
                'message': 'No active database connection'
            })
            
        cur = conn.cursor()
        cur.execute("""
            SELECT datname 
            FROM pg_database 
            WHERE datistemplate = false 
            ORDER BY datname
        """)
        databases = [db[0] for db in cur.fetchall()]
        cur.close()
        conn.close()
        
        return jsonify({
            'status': 'success',
            'databases': databases
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

def create_backup_with_psycopg2(db_params, db_name, backup_path, backup_type):
    """Create a backup using psycopg2 directly (fallback method)"""
    try:
        print(f"Creating backup using psycopg2 (fallback method)")
        
        # Connect to the database
        conn = psycopg2.connect(
            host=db_params['host'],
            port=5432,
            database=db_name,
            user=db_params['user'],
            password=db_params['password']
        )
        
        # Create a cursor
        cur = conn.cursor()
        
        # Open the output file
        with open(backup_path, 'w') as f:
            # Write header
            f.write(f"-- PostgreSQL database dump of {db_name}\n")
            f.write(f"-- Created at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            if backup_type in ['full', 'schema']:
                # Get schema information
                f.write("-- Schema\n\n")
                
                # Get tables
                try:
                    cur.execute("""
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY table_schema, table_name
                    """)
                    tables = cur.fetchall()
                    
                    for schema, table in tables:
                        f.write(f"-- Table: {schema}.{table}\n")
                        
                        # Get table creation SQL - this function might not exist in all PostgreSQL versions
                        # So we'll use a more compatible approach
                        try:
                            cur.execute(f"""
                                SELECT 'CREATE TABLE ' || 
                                       quote_ident(table_schema) || '.' || quote_ident(table_name) || ' (' ||
                                       string_agg(column_definition, ', ') || ');'
                                FROM (
                                    SELECT 
                                        column_name,
                                        data_type,
                                        quote_ident(column_name) || ' ' || 
                                        data_type || 
                                        CASE WHEN character_maximum_length IS NOT NULL 
                                             THEN '(' || character_maximum_length || ')' 
                                             ELSE '' END || 
                                        CASE WHEN is_nullable = 'NO' 
                                             THEN ' NOT NULL' 
                                             ELSE '' END as column_definition
                                    FROM information_schema.columns
                                    WHERE table_schema = '{schema}' AND table_name = '{table}'
                                    ORDER BY ordinal_position
                                ) t
                            """)
                            table_def = cur.fetchone()
                            if table_def and table_def[0]:
                                f.write(f"{table_def[0]}\n\n")
                            else:
                                f.write(f"-- No table definition found\n\n")
                        except Exception as table_err:
                            # If the above fails, just write a comment
                            f.write(f"-- Error getting table definition: {str(table_err)}\n\n")
                except Exception as schema_err:
                    f.write(f"-- Error getting schema information: {str(schema_err)}\n\n")
            
            if backup_type in ['full', 'data']:
                # Get data
                f.write("-- Data\n\n")
                
                # Get tables
                try:
                    cur.execute("""
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                        ORDER BY table_schema, table_name
                    """)
                    tables = cur.fetchall()
                    
                    for schema, table in tables:
                        f.write(f"-- Data for table: {schema}.{table}\n")
                        
                        try:
                            # Get column names
                            cur.execute(f"""
                                SELECT column_name
                                FROM information_schema.columns
                                WHERE table_schema = '{schema}' AND table_name = '{table}'
                                ORDER BY ordinal_position
                            """)
                            columns = [col[0] for col in cur.fetchall()]
                            
                            # Get data
                            cur.execute(f"SELECT * FROM {schema}.{table}")
                            rows = cur.fetchall()
                            
                            for row in rows:
                                values = []
                                for val in row:
                                    if val is None:
                                        values.append('NULL')
                                    elif isinstance(val, (int, float)):
                                        values.append(str(val))
                                    else:
                                        # Escape single quotes
                                        val_str = str(val).replace("'", "''")
                                        values.append(f"'{val_str}'")
                                
                                f.write(f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
                        except Exception as data_err:
                            f.write(f"-- Error getting data for table {schema}.{table}: {str(data_err)}\n")
                        
                        f.write("\n")
                except Exception as data_tables_err:
                    f.write(f"-- Error getting tables for data: {str(data_tables_err)}\n\n")
        
        # Close cursor and connection
        cur.close()
        conn.close()
        
        print(f"Backup created successfully at: {backup_path}")
        return True
    except Exception as e:
        print(f"Error creating backup with psycopg2: {e}")
        raise

@app.route('/create_pg_stat_statements', methods=['POST'])
@login_required
def create_pg_stat_statements():
    """Create the pg_stat_statements extension in the current database"""
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        try:
            # Check if the extension already exists
            cur.execute("""
                SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
            """)
            if cur.fetchone() is not None:
                flash("The pg_stat_statements extension is already created in this database.", "info")
            else:
                # Create the extension
                cur.execute("CREATE EXTENSION pg_stat_statements;")
                flash("Successfully created the pg_stat_statements extension!", "success")
        except Exception as e:
            flash(f"Failed to create pg_stat_statements extension: {str(e)}", "error")
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        flash(f"Database connection error: {str(e)}", "error")
    
    return redirect(url_for('performance'))

# Initialize SQLite database
init_sqlite_db()

# Migrate database schema if needed
migrate_db_schema()

# Initialize database on startup
init_db()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True) 