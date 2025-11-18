const API_AUTH_URL = 'http://localhost:8081'; // URL del microservicio de autenticación

const messageElement = document.getElementById('message');

function toggleForms() {
    const loginForm = document.getElementById('login-form');
    const signupForm = document.getElementById('signup-form');
    const toggleLink = document.querySelector('p[onclick="toggleForms()"]');

    if (loginForm.style.display !== 'none') {
        loginForm.style.display = 'none';
        signupForm.style.display = 'block';
        toggleLink.textContent = '¿Ya tienes cuenta? Inicia sesión';
    } else {
        loginForm.style.display = 'block';
        signupForm.style.display = 'none';
        toggleLink.textContent = '¿No tienes cuenta? Regístrate';
    }
    messageElement.textContent = '';
}

async function signup() {
    const username = document.getElementById('signup-username').value;
    const email = document.getElementById('signup-email').value;
    const password = document.getElementById('signup-password').value;
    messageElement.textContent = '';

    try {
        const response = await fetch(`${API_AUTH_URL}/signup`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, email, password }),
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Error en el registro.');
        }

        messageElement.style.color = 'green';
        messageElement.textContent = `¡Registro exitoso! Token: ${data.access_token}`;
        console.log('Token:', data.access_token);

    } catch (error) {
        messageElement.style.color = 'red';
        messageElement.textContent = error.message;
    }
}

async function login() {
    const username = document.getElementById('login-username').value;
    const password = document.getElementById('login-password').value;
    messageElement.textContent = '';

    try {
        const response = await fetch(`${API_AUTH_URL}/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username, password }),
        });

        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || 'Usuario o contraseña incorrectos.');
        }

        messageElement.style.color = 'green';
        messageElement.textContent = `¡Login exitoso! Token: ${data.access_token}`;
        console.log('Token:', data.access_token);
        // Aquí podrías guardar el token en localStorage y redirigir al usuario

    } catch (error) {
        messageElement.style.color = 'red';
        messageElement.textContent = error.message;
    }
}