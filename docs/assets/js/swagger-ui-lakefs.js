
window.onload = function() {
    // Begin Swagger UI call region
    const ui = SwaggerUIBundle({
        url: "../assets/js/swagger.yml",
        dom_id: '#swagger-ui',
        deepLinking: true,
        validatorUrl: null,
        supportedSubmitMethods: [],
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [],
        layout: "BaseLayout",
        onComplete: () => {
            if (!window.frameElement) return;
            window.frameElement.style.height = window.frameElement.contentWindow.document.documentElement.scrollHeight + 'px';
        },
    })
    // End Swagger UI call region

    window.ui = ui
}

function iframeLoaded() {
    var swagger_ui_frame = document.getElementById('swagger-ui-frame');
    if(swagger_ui_frame) {
    // here you can make the height, I delete it first, then I make it again
    swagger_ui_frame.height = "";
    swagger_ui_frame.height = swagger_ui_frame.contentWindow.document.body.scrollHeight + "px";
    }   
}
