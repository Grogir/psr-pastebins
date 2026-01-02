function getSelectedAuthors() {
    const boxes = document.querySelectorAll(".authorCheckbox");
    return Array.from(boxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);
}

function applyFilters() {
    const filter = document.getElementById("searchInput").value.toLowerCase();
    const allowedAuthors = getSelectedAuthors();
    const rows = document.querySelectorAll("#pastesTable tbody tr");

    rows.forEach(row => {
        const title = row.querySelector(".title").textContent.toLowerCase();
        const author = row.dataset.author;

        const matchTitle = title.includes(filter);
        const matchAuthor = allowedAuthors.includes(author);

        row.style.display = (matchTitle && matchAuthor) ? "" : "none";
    });
}

function toggleAuthors() {
    const boxes = document.querySelectorAll(".authorCheckbox");
    const allChecked = Array.from(boxes).every(cb => cb.checked);

    boxes.forEach(cb => cb.checked = !allChecked);
    applyFilters();
}
